(ns ziggurat.messaging.producer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [sentry.core :as sentry]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.util :refer [prefixed-queue-name]]
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
   (create-and-bind-queue queue-name exchange nil nil))
  ([queue-name exchange-name dead-letter-exchange queue-timeout-ms]
   (try
     (let [props (if (and dead-letter-exchange queue-timeout-ms)
                   {"x-dead-letter-exchange" dead-letter-exchange
                    "x-message-ttl"          queue-timeout-ms}
                   {})]
       (with-open [ch (lch/open connection)]
         (create-queue queue-name props ch)
         (declare-exchange ch exchange-name)
         (bind-queue-to-exchange ch queue-name exchange-name)))
     (catch Exception e
       (sentry/report-error sentry-reporter e "Error while declaring RabbitMQ queues")
       (throw e)))))

(defn- publish
  ([exchange message]
   (publish exchange "" message))
  ([exchange routing-key message]
   (with-retry {:count      3
                :wait       50
                :on-failure #(sentry/report-error sentry-reporter %
                                                  "Pushing message to rabbitmq failed")}
               (with-open [ch (lch/open connection)]
                 (lb/publish ch exchange routing-key (nippy/freeze message) {:content-type "application/octet-stream"
                                                                             :persistent   true})))))

(defn publish-to-delay-queue [topic-entity message]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message)))

(defn publish-to-dead-queue [topic-entity message]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message)))

(defn publish-to-instant-queue
  [topic-entity message]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (publish exchange-name message)))

(defn retry [{:keys [retry-count] :as message} topic-entity]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count)  (publish-to-delay-queue topic-entity (assoc message :retry-count (-> (ziggurat-config) :retry :count)))
      (pos? retry-count)  (publish-to-delay-queue topic-entity (assoc message :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-dead-queue topic-entity (dissoc message :retry-count)))))

(defn- make-delay-queue [topic-entity]
  (let [{:keys [queue-name exchange-name dead-letter-exchange queue-timeout-ms]} (:delay (rabbitmq-config))
        queue-name (delay-queue-name topic-entity queue-name)
        exchange-name (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange-name queue-timeout-ms)))

(defn- make-queue [topic-identifier queue-type]
  (let [{:keys [queue-name exchange-name]} (queue-type (rabbitmq-config))
        queue-name (prefixed-queue-name topic-identifier queue-name)
        exchange-name (prefixed-queue-name topic-identifier exchange-name)]
    (create-and-bind-queue queue-name exchange-name)))

(defn make-queues [stream-routes]
  (when (-> (ziggurat-config) :retry :enabled)
    (doseq [topic-entity (keys stream-routes)]
      (let [topic-name (name topic-entity)]
        (make-delay-queue topic-name)
        (make-queue topic-name :instant)
        (make-queue topic-name :dead-letter)))))
