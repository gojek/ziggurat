(ns ziggurat.messaging.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [sentry-clj.async :as sentry]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.messaging.connection :refer [connection is-connection-required?]]
            [ziggurat.messaging.util :refer :all]
            [ziggurat.retry :refer [with-retry]]
            [ziggurat.sentry :refer [sentry-reporter]]))

(defn delay-queue-name [topic-entity queue-name]
  (prefixed-queue-name topic-entity queue-name))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange)
  (log/infof "Bound queue %s to exchange %s" queue exchange))

(defn- create-and-bind-queue
  ([queue-name exchange]
   (create-and-bind-queue queue-name exchange nil))
  ([queue-name exchange-name dead-letter-exchange]
   (try
     (let [props (if dead-letter-exchange
                   {"x-dead-letter-exchange" dead-letter-exchange}
                   {})]
       (let [ch (lch/open connection)]
         (create-queue queue-name props ch)
         (declare-exchange ch exchange-name)
         (bind-queue-to-exchange ch queue-name exchange-name)))
     (catch Exception e
       (sentry/report-error sentry-reporter e "Error while declaring RabbitMQ queues")
       (throw e)))))

(defn- properties-for-publish
  [expiration]
  (let [props {:content-type "application/octet-stream"
               :persistent   true}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn- publish
  ([exchange message]
   (publish exchange message nil))
  ([exchange message expiration]
   (try
     (with-retry {:count      5
                  :wait       100
                  :on-failure #(log/error "publishing message to rabbitmq failed with error " (.getMessage %))}
       (with-open [ch (lch/open connection)]
         (lb/publish ch exchange "" (nippy/freeze message) (properties-for-publish expiration))))
     (catch Throwable e
       (sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message)))))

(defn- get-channel-retry-count [topic-entity channel]
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :count))

(defn- get-channel-retry-queue-timeout-ms [topic-entity channel]
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :queue-timeout-ms))

(defn- channel-retries-exponential-backoff-enabled [topic-entity channel]
  "Get exponential backoff enabled for specific channel from config."
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :exponential-backoff-enabled))

(defn- get-backoff-exponent [retry-count message-payload]
  "Get exponential-backoff value when exponential backoff is enabled. 
  Max exponent currently is hardoced to 10. For retrial 10th and more will use same queue timeout."
  (let [message-retry-count (:retry-count message-payload)
        exponent (- retry-count message-retry-count)]
    (if (< exponent 10)
      exponent
      10)))

(defn- get-backoff-exponent-timeout-ms [retry-count message-payload queue-timeout-ms]
  "Get queue timeout value when exponential backoff is enabled."
  (let [exponential-backoff (get-backoff-exponent retry-count message-payload)]
    (int (* (dec (Math/pow 2 exponential-backoff)) queue-timeout-ms))))

(defn get-queue-timeout-ms [message-payload]
  "Get queue timeout from config rabbitmq delay `queue-timeout-ms`.
   If `exponential-backoff-enabled` is true, `queue-timeout-ms` will be exponential with formula `(2^n)-1`, where `n` is message retry-count.
   _NOTE: Exponential backoff for channel retries is an experimental feature. It should not be used until released in a stable version._"
  (let [queue-timeout-ms (-> (rabbitmq-config) :delay :queue-timeout-ms)
        exponential-backoff-enabled (-> (ziggurat-config) :retry :exponential-backoff-enabled)
        retry-count (-> (ziggurat-config) :retry :count)]
    (if exponential-backoff-enabled
      (get-backoff-exponent-timeout-ms retry-count message-payload queue-timeout-ms)
      queue-timeout-ms)))

(defn get-channel-queue-timeout-ms [topic-entity channel message-payload]
  "Get queue timeout from config. It will use channel `queue-timeout-ms` if defined, otherwise it will use rabbitmq delay `queue-timeout-ms`.
   If `exponential-backoff-enabled` is true, `queue-timeout-ms` will be exponential with formula `(2^n)-1`, where `n` is message retry-count.
   _NOTE: Exponential backoff for channel retries is an experimental feature. It should not be used until released in a stable version._"
  (let [exponential-backoff-enabled (channel-retries-exponential-backoff-enabled topic-entity channel)
        queue-timeout-ms (-> (rabbitmq-config) :delay :queue-timeout-ms)
        channel-queue-timeout-ms (get-channel-retry-queue-timeout-ms topic-entity channel)
        retry-count (-> (ziggurat-config) :retry :count)
        channel-retry-count (get-channel-retry-count topic-entity channel)]
    (if exponential-backoff-enabled
      (get-backoff-exponent-timeout-ms (or channel-retry-count retry-count) message-payload (or channel-queue-timeout-ms queue-timeout-ms))
      (or channel-queue-timeout-ms queue-timeout-ms))))

(defn get-delay-exchange-name [topic-entity message-payload]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)
        exponential-backoff-enabled (-> (ziggurat-config) :retry :exponential-backoff-enabled)
        retry-count (-> (ziggurat-config) :retry :count)]
    (if exponential-backoff-enabled
      (let [backoff-exponent (get-backoff-exponent retry-count message-payload)]
        (prefixed-queue-name exchange-name backoff-exponent))
      exchange-name)))

(defn get-channel-delay-exchange-name [topic-entity channel message-payload]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)
        exponential-backoff-enabled (channel-retries-exponential-backoff-enabled topic-entity channel)
        channel-retry-count (get-channel-retry-count topic-entity channel)]
    (if exponential-backoff-enabled
      (let [exponential-backoff (get-backoff-exponent channel-retry-count message-payload)]
        (str (name exchange-name) "_" exponential-backoff))
      exchange-name)))

(defn publish-to-delay-queue [topic-entity message]
  (let [exchange-name (get-delay-exchange-name topic-entity message)
        queue-timeout-ms (get-queue-timeout-ms message)]
    (publish exchange-name message queue-timeout-ms)))

(defn publish-to-dead-queue [topic-entity message]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message)))

(defn publish-to-instant-queue
  [topic-entity message]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message)))

(defn publish-to-channel-delay-queue [topic-entity channel message]
  (let [exchange-name (get-channel-delay-exchange-name topic-entity channel message)
        queue-timeout-ms (get-channel-queue-timeout-ms topic-entity channel message)]
    (publish exchange-name message queue-timeout-ms)))

(defn publish-to-channel-dead-queue [topic-entity channel message]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message)))

(defn publish-to-channel-instant-queue
  [topic-entity channel message]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message)))

(defn- channel-retries-enabled [topic-entity channel]
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :enabled))

(defn retry [{:keys [retry-count] :as message} topic-entity]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count) (publish-to-delay-queue topic-entity (assoc message :retry-count (dec (-> (ziggurat-config) :retry :count))))
      (pos? retry-count) (publish-to-delay-queue topic-entity (assoc message :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-dead-queue topic-entity (dissoc message :retry-count)))))

(defn retry-for-channel [{:keys [retry-count] :as message} topic-entity channel]
  (when (channel-retries-enabled topic-entity channel)
    (cond
      (nil? retry-count) (publish-to-channel-delay-queue topic-entity channel (assoc message :retry-count (dec (get-channel-retry-count topic-entity channel))))
      (pos? retry-count) (publish-to-channel-delay-queue topic-entity channel (assoc message :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-channel-dead-queue topic-entity channel (dissoc message :retry-count)))))

(defn- make-delay-queue [topic-entity]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange-name)))

(defn- make-delay-with-retry-count-queue [topic-entity retry-count]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)
        sequence                  (if (<= retry-count 10) (inc retry-count) 11)]
    (doseq [s (range 1 sequence)]
      (create-and-bind-queue (prefixed-queue-name queue-name s) (prefixed-queue-name exchange-name s) dead-letter-exchange-name))))

(defn- make-channel-delay-with-retry-count-queue [topic-entity channel retry-count]
  (make-delay-with-retry-count-queue (with-channel-name topic-entity channel) retry-count))

(defn- make-channel-delay-queue [topic-entity channel]
  (make-delay-queue (with-channel-name topic-entity channel)))

(defn- make-queue [topic-identifier queue-type]
  (let [{:keys [queue-name exchange-name]} (queue-type (rabbitmq-config))
        queue-name    (prefixed-queue-name topic-identifier queue-name)
        exchange-name (prefixed-queue-name topic-identifier exchange-name)]
    (create-and-bind-queue queue-name exchange-name)))

(defn- make-channel-queue [topic-entity channel-name queue-type]
  (make-queue (with-channel-name topic-entity channel-name) queue-type))

(defn- make-channel-queues [channels topic-entity]
  (doseq [channel channels]
    (make-channel-queue topic-entity channel :instant)
    (when (channel-retries-enabled topic-entity channel)
      (make-channel-queue topic-entity channel :dead-letter)
      (if (channel-retries-exponential-backoff-enabled topic-entity channel)
        (make-channel-delay-with-retry-count-queue topic-entity channel (get-channel-retry-count topic-entity channel))
        (make-channel-delay-queue topic-entity channel)))))

(defn make-queues [stream-routes]
  (when (is-connection-required?)
    (doseq [topic-entity (keys stream-routes)]
      (let [channels (get-channel-names stream-routes topic-entity)]
        (make-channel-queues channels topic-entity)
        (when (-> (ziggurat-config) :retry :enabled)
          (make-queue topic-entity :instant)
          (make-queue topic-entity :dead-letter)
          (if (-> (ziggurat-config) :retry :exponential-backoff-enabled)
            (make-delay-with-retry-count-queue topic-entity (-> (ziggurat-config) :retry :count))
            (make-delay-queue topic-entity)))))))
