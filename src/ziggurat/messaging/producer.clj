(ns ziggurat.messaging.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.http :as lh]
            [langohr.queue :as lq]
            [ziggurat.messaging.channel_pool :as cpool]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [config ziggurat-config rabbitmq-config channel-retry-config]]
            [ziggurat.messaging.connection :refer [connection is-connection-required?]]
            [ziggurat.messaging.util :as util]
            [ziggurat.metrics :as metrics])
  (:import (com.rabbitmq.client AlreadyClosedException)
           (java.io IOException)))

(def MAX_EXPONENTIAL_RETRIES 25)

(defn delay-queue-name [topic-entity queue-name]
  (util/prefixed-queue-name topic-entity queue-name))

(defn get-replica-count [host-count]
  (int (Math/ceil (/ host-count 2))))

(defn get-default-ha-policy [rmq-config replica-count]
  (let [ha-mode      (get rmq-config :ha-mode "exactly")
        ha-params    (get rmq-config :ha-params replica-count)
        ha-sync-mode (get rmq-config :ha-sync-mode "automatic")]
    (if (= "all" ha-mode)
      {:ha-mode "all" :ha-sync-mode ha-sync-mode}
      {:ha-mode ha-mode :ha-sync-mode ha-sync-mode :ha-params ha-params})))

(defn set-ha-policy-on-host [host-endpoint username password ha-policy-body exchange-name queue-name]
  (try
    (binding [lh/*endpoint* host-endpoint
              lh/*username* username
              lh/*password* password]
      (log/info "applying HA policies to queue: " queue-name)
      (log/info "applying HA policies to exchange: " exchange-name)
      (lh/set-policy "/" (str queue-name "_ha_policy")
                     {:apply-to   "all"
                      :pattern    (str "^" queue-name "|" exchange-name "$")
                      :definition ha-policy-body}))
    (catch Exception e
      (log/error "error setting ha-policies" (.getMessage e))
      nil)))

(defn set-ha-policy [queue-name exchange-name rmq-config]
  (let [username       (:username rmq-config)
        password       (:password rmq-config)
        hosts-vec      (util/list-of-hosts rmq-config)
        ha-policy-body (get-default-ha-policy rmq-config (get-replica-count (count hosts-vec)))]
    (loop [hosts hosts-vec]
      (let [host-endpoint   (str "http://" (first hosts) ":" (get rmq-config :admin-port 15672))
            resp            (set-ha-policy-on-host host-endpoint username password ha-policy-body exchange-name queue-name)
            remaining-hosts (rest hosts)]
        (when (and (nil? resp)
                   (pos? (count remaining-hosts)))
          (recur remaining-hosts))))))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

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
                   {})
           ch    (lch/open connection)]
       (create-queue queue-name props ch)
       (declare-exchange ch exchange-name)
       (bind-queue-to-exchange ch queue-name exchange-name)
       (set-ha-policy queue-name exchange-name (get-in config [:ziggurat :rabbit-mq-connection])))
     (catch Exception e
       (log/error e "Error while declaring RabbitMQ queues")
       (throw e)))))

(defn- record-headers->map [record-headers]
  (reduce (fn [header-map record-header]
            (assoc header-map (.key record-header) (String. (.value record-header))))
          {}
          record-headers))

(defn- properties-for-publish
  [expiration headers]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      (record-headers->map headers)}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn- handle-network-exception
  [e message-payload]
  (log/error e "Exception was encountered while publishing to RabbitMQ")
  (metrics/increment-count ["rabbitmq" "publish" "network"] "exception" {:topic-entity (name (:topic-entity message-payload))})
  true)

(defn- publish-internal
  [exchange message-payload expiration]
  (try
    (let [ch (.borrowObject cpool/channel-pool)]
      (try
        (lb/publish ch exchange "" (nippy/freeze (dissoc message-payload :headers))
                    (properties-for-publish expiration (:headers message-payload)))
        false
        (catch AlreadyClosedException e
          (handle-network-exception e message-payload))
        (catch IOException e
          (handle-network-exception e message-payload))
        (catch Exception e
          (log/error e "Exception was encountered while publishing to RabbitMQ")
          (metrics/increment-count ["rabbitmq" "publish"] "exception" {:topic-entity (name (:topic-entity message-payload))})
          false)
        (finally (.returnObject cpool/channel-pool ch))))
    (catch Exception e
      (log/error "Exception occurred while borrowing a channel from the pool")
      (metrics/increment-count ["rabbitmq" "publish" "channel_borrow"] {:topic-entity (name (:topic-entity message-payload))}))))

(defn publish
  ([exchange message-payload]
   (publish exchange message-payload nil))
  ([exchange message-payload expiration]
   (when (publish-internal exchange message-payload expiration)
     (Thread/sleep 5000)
     (log/info "Retrying publishing the message to " exchange)
     (recur exchange message-payload expiration))))

(defn- retry-type []
  (-> (ziggurat-config) :retry :type))

(defn- channel-retries-enabled [topic-entity channel]
  (:enabled (channel-retry-config topic-entity channel)))

(defn- channel-retry-type [topic-entity channel]
  (:type (channel-retry-config topic-entity channel)))

(defn- get-channel-retry-count [topic-entity channel]
  (:count (channel-retry-config topic-entity channel)))

(defn- get-channel-queue-timeout-or-default-timeout [topic-entity channel]
  (let [channel-queue-timeout-ms (:queue-timeout-ms (channel-retry-config topic-entity channel))
        queue-timeout-ms         (get-in (rabbitmq-config) [:delay :queue-timeout-ms])]
    (or channel-queue-timeout-ms queue-timeout-ms)))

(defn- get-backoff-exponent
  "Calculates the exponent using the formula `retry-count` and `message-retry-count`, where `retry-count` is the total retries
   possible and `message-retry-count` is the count of retries available for the message.

   Caps the value of `retry-count` to MAX_EXPONENTIAL_RETRIES.

   Returns 1, if `message-retry-count` is higher than `max(MAX_EXPONENTIAL_RETRIES, retry-count)`."
  [retry-count message-retry-count]
  (let [exponent (- (min MAX_EXPONENTIAL_RETRIES retry-count) message-retry-count)]
    (max 1 exponent)))

(defn- get-exponential-backoff-timeout-ms "Calculates the exponential timeout value from the number of max retries possible (`retry-count`),
   the number of retries available for a message (`message-retry-count`) and base timeout value (`queue-timeout-ms`).
   It uses this formula `((2^n)-1)*queue-timeout-ms`, where `n` is the current message retry-count.

   Sample config to use exponential backoff:
   {:ziggurat {:retry {:enabled true
                       :count   5
                       :type    :exponential}}}

   Sample config to use exponential backoff when using channel flow:
   {:ziggurat {:stream-router {topic-entity {:channels {channel {:retry {:count 5
                                                                         :enabled true
                                                                         :queue-timeout-ms 1000
                                                                         :type :exponential}}}}}}}

   _NOTE: Exponential backoff for channel retries is an experimental feature. It should not be used until released in a stable version._"
  [retry-count message-retry-count queue-timeout-ms]
  (let [exponential-backoff (get-backoff-exponent retry-count message-retry-count)]
    (long (* (dec (Math/pow 2 exponential-backoff)) queue-timeout-ms))))

(defn get-queue-timeout-ms
  "Calculate queue timeout for delay queue. Uses the value from [[get-exponential-backoff-timeout-ms]] if exponential backoff enabled."
  [message-payload]
  (let [queue-timeout-ms    (-> (rabbitmq-config) :delay :queue-timeout-ms)
        retry-count         (-> (ziggurat-config) :retry :count)
        message-retry-count (:retry-count message-payload)]
    (if (= :exponential (-> (ziggurat-config) :retry :type))
      (get-exponential-backoff-timeout-ms retry-count message-retry-count queue-timeout-ms)
      queue-timeout-ms)))

(defn get-channel-queue-timeout-ms
  "Calculate queue timeout for channel delay queue. Uses the value from [[get-exponential-backoff-timeout-ms]] if exponential backoff enabled."
  [topic-entity channel message-payload]
  (let [channel-queue-timeout-ms (get-channel-queue-timeout-or-default-timeout topic-entity channel)
        message-retry-count      (:retry-count message-payload)
        channel-retry-count      (get-channel-retry-count topic-entity channel)]
    (if (= :exponential (channel-retry-type topic-entity channel))
      (get-exponential-backoff-timeout-ms channel-retry-count message-retry-count channel-queue-timeout-ms)
      channel-queue-timeout-ms)))

(defn get-delay-exchange-name
  "This function return delay exchange name for retry when using flow without channel. It will return exchange name with retry count as suffix if exponential backoff enabled."
  [topic-entity message-payload]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name (util/prefixed-queue-name topic-entity exchange-name)
        retry-count   (-> (ziggurat-config) :retry :count)]
    (if (= :exponential (-> (ziggurat-config) :retry :type))
      (let [message-retry-count (:retry-count message-payload)
            backoff-exponent    (get-backoff-exponent retry-count message-retry-count)]
        (util/prefixed-queue-name exchange-name backoff-exponent))
      exchange-name)))

(defn get-channel-delay-exchange-name
  "This function return delay exchange name for retry when using channel flow. It will return exchange name with retry count as suffix if exponential backoff enabled."
  [topic-entity channel message-payload]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name       (util/prefixed-channel-name topic-entity channel exchange-name)
        channel-retry-count (get-channel-retry-count topic-entity channel)]
    (if (= :exponential (channel-retry-type topic-entity channel))
      (let [message-retry-count (:retry-count message-payload)
            exponential-backoff (get-backoff-exponent channel-retry-count message-retry-count)]
        (str (name exchange-name) "_" exponential-backoff))
      exchange-name)))

(defn publish-to-delay-queue [message-payload]
  (let [topic-entity     (:topic-entity message-payload)
        exchange-name    (get-delay-exchange-name topic-entity message-payload)
        queue-timeout-ms (get-queue-timeout-ms message-payload)]
    (publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-dead-queue [message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (util/prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-instant-queue [message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (util/prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-channel-delay-queue [channel message-payload]
  (let [topic-entity     (:topic-entity message-payload)
        exchange-name    (get-channel-delay-exchange-name topic-entity channel message-payload)
        queue-timeout-ms (get-channel-queue-timeout-ms topic-entity channel message-payload)]
    (publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-channel-dead-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (util/prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-channel-instant-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (util/prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message-payload)))

(defn retry [{:keys [retry-count] :as message-payload}]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec (-> (ziggurat-config) :retry :count))))
      (pos? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-dead-queue (assoc message-payload :retry-count (-> (ziggurat-config) :retry :count))))))

(defn retry-for-channel [{:keys [retry-count topic-entity] :as message-payload} channel]
  (when (channel-retries-enabled topic-entity channel)
    (cond
      (nil? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec (get-channel-retry-count topic-entity channel))))
      (pos? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-channel-dead-queue channel (assoc message-payload :retry-count (get-channel-retry-count topic-entity channel))))))

(defn- make-delay-queue [topic-entity]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (util/prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (util/prefixed-queue-name topic-entity dead-letter-exchange)]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange-name)))

(defn- make-delay-queue-with-retry-count [topic-entity retry-count]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (util/prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (util/prefixed-queue-name topic-entity dead-letter-exchange)
        sequence                  (min MAX_EXPONENTIAL_RETRIES (inc retry-count))]
    (doseq [s (range 1 sequence)]
      (create-and-bind-queue (util/prefixed-queue-name queue-name s) (util/prefixed-queue-name exchange-name s) dead-letter-exchange-name))))

(defn- make-channel-delay-queue-with-retry-count [topic-entity channel retry-count]
  (make-delay-queue-with-retry-count (util/with-channel-name topic-entity channel) retry-count))

(defn- make-channel-delay-queue [topic-entity channel]
  (make-delay-queue (util/with-channel-name topic-entity channel)))

(defn- make-queue [topic-identifier queue-type]
  (let [{:keys [queue-name exchange-name]} (queue-type (rabbitmq-config))
        queue-name    (util/prefixed-queue-name topic-identifier queue-name)
        exchange-name (util/prefixed-queue-name topic-identifier exchange-name)]
    (create-and-bind-queue queue-name exchange-name)))

(defn- make-channel-queue [topic-entity channel-name queue-type]
  (make-queue (util/with-channel-name topic-entity channel-name) queue-type))

(defn- make-channel-queues [channels topic-entity]
  (doseq [channel channels]
    (make-channel-queue topic-entity channel :instant)
    (when (channel-retries-enabled topic-entity channel)
      (make-channel-queue topic-entity channel :dead-letter)
      (let [channel-retry-type (channel-retry-type topic-entity channel)]
        (cond
          (= :exponential channel-retry-type) (do
                                                (log/warn "[Alpha Feature]: Exponential backoff based retries is an alpha feature."
                                                          "Please use it only after understanding its risks and implications."
                                                          "Its contract can change in the future releases of Ziggurat.")
                                                (make-channel-delay-queue-with-retry-count topic-entity channel (get-channel-retry-count topic-entity channel)))
          (= :linear channel-retry-type) (make-channel-delay-queue topic-entity channel)
          (nil? channel-retry-type) (do
                                      (log/warn "[Deprecation Notice]: Please note that the configuration for channel retries has changed."
                                                "Please look at the upgrade guide for details: https://github.com/gojek/ziggurat/wiki/Upgrade-guide"
                                                "Use :type to specify the type of retry mechanism in the channel config.")
                                      (make-channel-delay-queue topic-entity channel))
          :else (do
                  (log/warn "Incorrect keyword for type passed, falling back to linear backoff for channel: " channel)
                  (make-channel-delay-queue topic-entity channel)))))))

(defn make-queues [routes]
  (when (is-connection-required?)
    (doseq [topic-entity (keys routes)]
      (let [channels   (util/get-channel-names routes topic-entity)
            retry-type (retry-type)]
        (make-channel-queues channels topic-entity)
        (when (-> (ziggurat-config) :retry :enabled)
          (make-queue topic-entity :instant)
          (make-queue topic-entity :dead-letter)
          (cond
            (= :exponential retry-type) (do
                                          (log/warn "[Alpha Feature]: Exponential backoff based retries is an alpha feature."
                                                    "Please use it only after understanding its risks and implications."
                                                    "Its contract can change in the future releases of Ziggurat.")
                                          (make-delay-queue-with-retry-count topic-entity (-> (ziggurat-config) :retry :count)))
            (= :linear retry-type) (make-delay-queue topic-entity)
            (nil? retry-type) (do
                                (log/warn "[Deprecation Notice]: Please note that the configuration for retries has changed."
                                          "Please look at the upgrade guide for details: https://github.com/gojek/ziggurat/wiki/Upgrade-guide"
                                          "Use :type to specify the type of retry mechanism in the config.")
                                (make-delay-queue topic-entity))
            :else (do
                    (log/warn "Incorrect keyword for type passed, falling back to linear backoff for topic Entity: " topic-entity)
                    (make-delay-queue topic-entity))))))))
