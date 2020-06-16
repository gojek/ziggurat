(ns ziggurat.messaging.rabbitmq.consumer
  (:require [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [ziggurat.messaging.rabbitmq.connection :refer [connection]]
            [langohr.consumers :as lcons]))

(defn prefixed-queue-name [topic-entity value]
  (str (name topic-entity) "_" value))

(defn with-channel-name [topic-entity channel]
  (str (name topic-entity) "_channel_" (name channel)))

(defn prefixed-channel-name [topic-entity channel-name value]
  (prefixed-queue-name (with-channel-name topic-entity channel-name)
                       value))

;; end of util ns

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
