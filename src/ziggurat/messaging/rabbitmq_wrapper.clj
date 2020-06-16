(ns ziggurat.messaging.rabbitmq-wrapper
  (:require [ziggurat.config :refer [get-in-config]]
            [mount.core :refer [defstate]]
            [sentry-clj.async :as sentry]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.retry :refer [with-retry]]
            [clojure.tools.logging :as log]
            [ziggurat.messaging.util :refer :all]
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
        (log/error e "Error while starting RabbitMQ connection")
        (throw e)))))

(defn- stop-connection [conn ziggurat-config stream-routes]
  (when (is-connection-required? ziggurat-config stream-routes)
    (if (get-in  ziggurat-config [:tracer :enabled])
      (.close conn)
      (rmq/close conn))
    (log/info "Disconnected from RabbitMQ")))


(defstate connection
  :start (start-connection (ziggurat-config) (:stream-routes (mount/args)))
  :stop (stop-connection connection (ziggurat-config) (:stream-routes (mount/args))))

;;End of connection namespace

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
       (log/error e "Pushing message to rabbitmq failed, data: " message-payload)
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
  (log/infof "Bound queue %s to exchange %s" queue exchange))

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
       (log/error e "Error while declaring RabbitMQ queues")
       (throw e)))))

;; End of producer namespace

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn consume-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and acks the message
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

(defn get-message-from-queue [ch queue-name ack?]
  (let [[meta payload] (lb/get ch queue-name false)]
    (when (some? payload)
      (consume-message ch meta payload ack?))))

(defn process-message-from-queue [ch meta payload processing-fn]
  (let [delivery-tag (:delivery-tag meta)
        message-payload      (consume-message ch meta payload false)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling processor-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (processing-fn message-payload)
        (ack-message ch delivery-tag)
        :success
        (catch Exception e
          ;TODO fix this error
          ; Channels get closed by the client if there is an exception. We are going to restart the channel to reject the message
          (lb/reject ch delivery-tag true)
          ;(sentry/report-error sentry-reporter e "Error while processing message-payload from RabbitMQ")
          :failed)))))

(defn- message-handler [wrapped-mapper-fn]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload wrapped-mapper-fn)))

(defn- get-instant-queue-name [topic-entity ziggurat-config]
  (let [instant-queue-name (get-in ziggurat-config [:rabbit-mq :instant :queue-name])]
    (prefixed-queue-name topic-entity instant-queue-name)))

(defn- get-channel-instant-queue-name [topic-entity channel-key ziggurat-config]
  (let [instant-queue-name (get-in ziggurat-config [:rabbit-mq :instant :queue-name])]
    (prefixed-channel-name topic-entity channel-key instant-queue-name)))

(defn get-dead-set-queue-name
  ([topic-entity ziggurat-config]
   (get-dead-set-queue-name topic-entity ziggurat-config nil))
  ([topic-entity ziggurat-config channel]
   (if (nil? channel)
     (prefixed-queue-name topic-entity (get-in ziggurat-config [:rabbit-mq :dead-letter :queue-name]))
     (prefixed-channel-name topic-entity channel (get-in ziggurat-config [:rabbit-mq :dead-letter :queue-name])))))

(defn start-subscriber [prefetch-count wrapped-mapper-fn topic-entity channel-key ziggurat-config]
  (let [ch (lch/open connection)
        queue-name (if (some? channel-key)
                     (get-channel-instant-queue-name topic-entity channel-key ziggurat-config)
                     (get-instant-queue-name topic-entity ziggurat-config))
        _ (lb/qos ch prefetch-count)
        consumer-tag (lcons/subscribe ch
                                      queue-name
                                      (message-handler wrapped-mapper-fn)
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                                       :handle-consume-ok-fn      (fn [consumer_tag]
                                                                    (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))})]))

(defn get-messages-from-queue
  ([queue-name ack?] (get-messages-from-queue queue-name ack? 1))
  ([queue-name ack? count]
   (with-open [ch (lch/open connection)]
     (doall
      (for [_ (range count)]
        (try
          (get-message-from-queue ch queue-name ack?)
          (catch Exception e
            (log/error e))))))))

(defn process-messages-from-queue [queue-name count processing-fn]
  (with-open [ch (lch/open connection)]
    (doall
     (for [_ (range count)]
       (let [[meta payload] (lb/get ch queue-name false)]
         (process-message-from-queue ch meta payload processing-fn))))))

;; End of consumer namespace

