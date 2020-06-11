(ns ziggurat.messaging.rabbitmq-wrapper
  (:require [ziggurat.config :refer [get-in-config]]
            [mount.core :refer [defstate]]
            [ziggurat.metrics :as metrics]
            [sentry-clj.async :as sentry]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.retry :refer [with-retry]]
            [clojure.tools.logging :as log]
            [schema.core :as s]
            [ziggurat.messaging.util :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.tracer :refer [tracer]]
            [langohr.consumers :as lcons]
            [langohr.channel :as lch]
            [ziggurat.channel :refer [get-keys-for-topic]]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.core :as rmq]
            [mount.core :as mount])
  (:import (io.opentracing.contrib.rabbitmq TracingConnectionFactory)
           (com.rabbitmq.client ListAddressResolver Address ShutdownListener)
           (com.rabbitmq.client.impl DefaultCredentialsProvider)
           (java.util.concurrent ExecutorService Executors)))

(defn is-connection-required? [ziggurat-config stream-routes]
  (let [all-channels  (reduce (fn [all-channel-vec [topic-entity _]]
                                (concat all-channel-vec (get-keys-for-topic stream-routes topic-entity)))
                              []
                              stream-routes)]
    (or (pos? (count all-channels))
        (-> ziggurat-config :retry :enabled))))

(defn- channel-threads [channels]
  (reduce (fn [sum [_ channel-config]]
            (+ sum (:worker-count channel-config))) 0 channels))

(defn- total-thread-count [ziggurat-config]
  (let [stream-routes (:stream-router  ziggurat-config)
        worker-count  (get-in  ziggurat-config [:jobs :instant :worker-count])]
    (reduce (fn [sum [_ route-config]]
              (+ sum (channel-threads (:channels route-config)) worker-count)) 0 stream-routes)))

(defn- get-config-for-rabbitmq [ziggurat-config]
  (assoc (:rabbit-mq-connection ziggurat-config) :executor (Executors/newFixedThreadPool (total-thread-count ziggurat-config))))

(defn create-connection [config tracer-enabled]
  (if tracer-enabled
    (let [connection-factory (TracingConnectionFactory. tracer)]
      (.setCredentialsProvider connection-factory (DefaultCredentialsProvider. (:username config) (:password config)))
      (.newConnection connection-factory ^ExecutorService (:executor config) ^ListAddressResolver (ListAddressResolver. (list (Address. (:host config) (:port config))))))

    (rmq/connect config)))

(defn- start-connection [ziggurat-config stream-routes]
  (log/info "Connecting to RabbitMQ")
  (when (is-connection-required? ziggurat-config stream-routes)
    (try
      (let [connection (create-connection (get-config-for-rabbitmq ziggurat-config) (get-in ziggurat-config [:tracer :enabled]))]
        (doto connection
          (.addShutdownListener
           (reify ShutdownListener
             (shutdownCompleted [_ cause]
               (when-not (.isInitiatedByApplication cause)
                 (log/error cause "RabbitMQ connection shut down due to error")))))))
      (catch Exception e
        (sentry/report-error sentry-reporter e "Error while starting RabbitMQ connection")
        (throw e)))))

(defn- stop-connection [conn ziggurat-config stream-routes]
  (when (is-connection-required? ziggurat-config stream-routes)
    (if (get-in  ziggurat-config [:tracer :enabled])
      (.close conn)
      (rmq/close conn))
    (log/info "Disconnected from RabbitMQ")))

(defn- record-headers->map [record-headers]
  (reduce (fn [header-map record-header]
            (assoc header-map (.key record-header) (String. (.value record-header))))
          {}
          record-headers))

;;End of connection namespace

(defstate connection
  :start (start-connection (ziggurat-config) (:stream-routes (mount/args)))
  :stop (stop-connection connection (ziggurat-config) (:stream-routes (mount/args))))

(defn- properties-for-publish
  [expiration headers]
  (let [props {:content-type "application/octet-stream"
               :persistent   true
               :headers      (record-headers->map headers)}]
    (if expiration
      (assoc props :expiration (str expiration))
      props)))

(defn publish
  ([exchange message-payload]
   (publish exchange message-payload nil))
  ([exchange message-payload expiration]
   (try
     (with-retry {:count      5
                  :wait       100
                  :on-failure #(log/error "publishing message to rabbitmq failed with error " (.getMessage %))}
       (with-open [ch (lch/open connection)]
         (lb/publish ch exchange "" (nippy/freeze (dissoc message-payload :headers)) (properties-for-publish expiration (:headers message-payload)))))
     (catch Throwable e
       (sentry/report-error sentry-reporter e
                            "Pushing message to rabbitmq failed, data: " message-payload)
       (throw (ex-info "Pushing message to rabbitMQ failed after retries, data: " {:type :rabbitmq-publish-failure
                                                                                   :error e}))))))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange)
  (log/info "Bound queue %s to exchange %s" queue exchange))

(defn create-and-bind-queue
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

;; End of publish namespace

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn consume-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and converts it to `MessagePayload`. Acks the message
  if `ack?` is true."
  [ch {:keys [delivery-tag]} ^bytes payload ack?]
  (try
    (let [message (nippy/thaw payload)]
      (when ack?
        (lb/ack ch delivery-tag))
      message)
    (catch Exception e
      (lb/reject ch delivery-tag false)
      (log/error "error fetching the message from rabbitmq " e)
      nil)))

(defn process-message-from-queue [ch meta payload topic-entity processing-fn]
  (let [delivery-tag (:delivery-tag meta)
        message-payload      (consume-message ch meta payload false)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling processor-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (processing-fn message-payload)
        (ack-message ch delivery-tag)
        (catch Exception e
          (lb/reject ch delivery-tag true)
          (sentry/report-error sentry-reporter e "Error while processing message-payload from RabbitMQ")
          (metrics/increment-count ["rabbitmq-message" "process"] "failure" {:topic_name (name topic-entity)}))))))

(defn- message-handler [wrapped-mapper-fn topic-entity]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload topic-entity wrapped-mapper-fn)))

(defn start-subscriber [ch prefetch-count queue-name wrapped-mapper-fn topic-entity]
  (lb/qos ch prefetch-count)
  (let [consumer-tag (lcons/subscribe ch
                                      queue-name
                                      (message-handler wrapped-mapper-fn topic-entity)
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                                       :handle-consume-ok-fn      (fn [consumer_tag]
                                                                    (log/info "consumer started for %s with consumer tag %s " queue-name consumer_tag))})]))

;; End of consumer namespace

