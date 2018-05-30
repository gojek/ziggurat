(ns ziggurat.messaging.producer
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.messaging.connection :refer [connection]]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [mount.core :refer [defstate]]
            [sentry.core :as sentry]
            [ziggurat.retry :refer [with-retry]]
            [taoensso.nippy :as nippy]
            [executor.core :as executor])
  (:import [com.rabbitmq.client AlreadyClosedException ShutdownListener]
           (java.util.concurrent ExecutorService TimeUnit)))


(defn delay-queue-name [queue-name queue-timeout-ms]
  (format "%s_%s" queue-name queue-timeout-ms))

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

(defn publish-to-delay-queue [message]
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))]
    (publish exchange-name message)))

(defn publish-to-dead-queue [message]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))]
    (publish exchange-name message)))

(defn publish-to-instant-queue [message]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))]
    (publish exchange-name message)))

(defn retry [{:keys [retry-count] :as message}]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count) (publish-to-delay-queue (assoc message :retry-count (-> (ziggurat-config) :retry :count)))
      (> retry-count 0)  (publish-to-delay-queue (assoc message :retry-count (dec retry-count)))
      (= retry-count 0)  (publish-to-dead-queue (dissoc message :retry-count)))))

(defn- make-delay-queue []
  (let [{:keys [queue-name exchange-name dead-letter-exchange queue-timeout-ms]} (:delay (rabbitmq-config))
        queue-name (delay-queue-name queue-name queue-timeout-ms)]
    (create-and-bind-queue queue-name exchange-name dead-letter-exchange queue-timeout-ms)))

(defn- make-instant-queue []
  (let [{:keys [queue-name exchange-name]} (:instant (rabbitmq-config))]
    (create-and-bind-queue queue-name exchange-name)))

(defn- make-dead-letter-queue []
  (let [{:keys [queue-name exchange-name]} (:dead-letter (rabbitmq-config))]
    (create-and-bind-queue queue-name exchange-name)))

(defn make-queues []
  (when (-> (ziggurat-config) :retry :enabled)
    (make-delay-queue)
    (make-instant-queue)
    (make-dead-letter-queue)))
