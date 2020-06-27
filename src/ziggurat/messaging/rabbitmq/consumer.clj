(ns ziggurat.messaging.rabbitmq.consumer
  (:require [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [clojure.tools.logging :as log]
            [ziggurat.retry :refer [with-retry]]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]))

(defn- ack-message
  "created this to mock lb/ack methods in test, we are not able to directly mock out lb/ack because of type annotations in the fn definition"
  ; TODO: try to figure out a way to mock lb/ack to remove this fn.
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn- reject-message
  "created this to mock lb/reject methods in test, we are not able to directly mock out lb/reject because of type annotations in the fn definition"
  ; TODO: try to figure out a way to mock lb/reject to remove this fn.
  [ch delivery-tag requeue]
  (lb/reject ch delivery-tag requeue))

(defn consume-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and acks the message
  if `ack?` is true."
  [ch {:keys [delivery-tag]} ^bytes payload ack?]
  (try
    (let [message (nippy/thaw payload)]
      (when ack?
        (ack-message ch delivery-tag))
      message)
    (catch Exception e
      (reject-message ch delivery-tag false)
      (log/error "error fetching the message from rabbitmq " e)
      nil)))

(defn- get-message-from-queue [ch queue-name ack?]
  (let [[meta payload] (lb/get ch queue-name false)]
    (when (some? payload)
      (consume-message ch meta payload ack?))))

(defn process-message-from-queue [ch meta payload processing-fn]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (consume-message ch meta payload false)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling processor-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (processing-fn message-payload)
        (ack-message ch delivery-tag)
        (catch Exception e
          ;TODO Channels get closed by the client if there is an exception. Channel has to be re-opened to reject the msg
          (reject-message ch delivery-tag true)
          (log/error "Error while processing message-payload from RabbitMQ" (str "\nError: " e))
          nil)))))

(defn- message-handler [wrapped-mapper-fn]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload wrapped-mapper-fn)))

(defn start-subscriber [connection prefetch-count wrapped-mapper-fn queue-name]
  (let [ch           (lch/open connection)
        _            (lb/qos ch prefetch-count)
        consumer-tag (lcons/subscribe ch
                                      queue-name
                                      (message-handler wrapped-mapper-fn)
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                                       :handle-consume-ok-fn      (fn [consumer_tag]
                                                                    (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))})]))

(defn get-messages-from-queue [connection queue-name ack? count]
  (with-open [ch (lch/open connection)]
    (doall
     (for [_ (range count)]
       (try
         (get-message-from-queue ch queue-name ack?)
         (catch Exception e
           (log/error e)))))))

(defn process-messages-from-queue [connection queue-name count processing-fn]
  (with-open [ch (lch/open connection)]
    (doall
     (for [_ (range count)]
       (let [[meta payload] (lb/get ch queue-name false)]
         (process-message-from-queue ch meta payload processing-fn))))))
