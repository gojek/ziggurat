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
  ([exchange message-payload]
   (publish exchange message-payload nil))
  ([exchange message-payload expiration]
   (try
     (with-retry {:count      5
                  :wait       100
                  :on-failure #(log/error "publishing message to rabbitmq failed with error " (.getMessage %))}
       (with-open [ch (lch/open connection)]
         (lb/publish ch exchange "" (nippy/freeze message-payload) (properties-for-publish expiration))))
     (catch Throwable e
       (sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message-payload)))))

(defn publish-to-delay-queue [message-payload]
  (let [{:keys [exchange-name queue-timeout-ms]} (:delay (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-dead-queue [message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-instant-queue [message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-channel-delay-queue [channel message-payload]
  (let [{:keys [exchange-name queue-timeout-ms]} (:delay (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-channel-dead-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message-payload)))

(defn publish-to-channel-instant-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity (:topic-entity message-payload)
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (publish exchange-name message-payload)))

(defn- channel-retries-enabled [topic-entity channel]
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :enabled))

(defn- get-channel-retry-count [topic-entity channel]
  (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :count))

(defn retry [{:keys [retry-count topic-entity] :as message-payload}]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec (-> (ziggurat-config) :retry :count))))
      (pos? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-dead-queue message-payload))))

(defn retry-for-channel [{:keys [retry-count topic-entity] :as message-payload} channel]
  (when (channel-retries-enabled topic-entity channel)
    (cond
      (nil? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec (get-channel-retry-count topic-entity channel))))
      (pos? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-channel-dead-queue channel message-payload))))

(defn- make-delay-queue [topic-entity]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange-name)))

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
      (make-channel-delay-queue topic-entity channel)
      (make-channel-queue topic-entity channel :dead-letter))))

(defn make-queues [stream-routes]
  (when (is-connection-required?)
    (doseq [topic-entity (keys stream-routes)]
      (let [channels (get-channel-names stream-routes topic-entity)]
        (make-channel-queues channels topic-entity)
        (when (-> (ziggurat-config) :retry :enabled)
          (make-delay-queue topic-entity)
          (make-queue topic-entity :instant)
          (make-queue topic-entity :dead-letter))))))
