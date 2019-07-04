(ns ziggurat.messaging.consumer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [schema.core :as s]
            [sentry-clj.async :as sentry]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [get-in-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.messaging.util :refer :all])
  (:import [com.rabbitmq.client AlreadyClosedException Channel]))

(defn- convert-to-message-payload
  "checks if the message is a message payload or a message(pushed by Ziggurat version < 3.0.0) and converts messages to message-payload to pass onto the mapper-fn.
  This function is used for migration from Ziggurat Version 2.x to 3.x"
  [message]
  (try
    (s/validate mpr/message-payload message)
    (catch Exception e
      (log/info "old message format read, converting to message-payload: " message)
      (let [retry-count (:retry-count message)
            message-payload (mpr/construct-message-payload (dissoc message :retry-count))]
        (assoc message-payload :retry-count retry-count)))))

(defn convert-and-ack-message
  "Take the ch metadata payload and ack? as parameter.
   Decodes the payload the ack it if ack is enabled and returns the message"
  [ch {:keys [delivery-tag] :as meta} ^bytes payload ack?]
  (try
    (let [message (nippy/thaw payload)]
      (log/debug "Calling mapper fn with the message - " message " with retry count - " (:retry-count message))
      (when ack?
        (lb/ack ch delivery-tag))
      (convert-to-message-payload message))
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while decoding message")
      (lb/reject ch delivery-tag false)
      nil)))

(defn- try-consuming-dead-set-messages [ch ack? queue-name]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (convert-and-ack-message ch meta payload ack?)))
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while consuming the dead set message"))))

(defn- get-dead-set-messages*
  "Get the n(count) messages from the rabbitmq.

   If ack is set to true,
   then ack all the messages while consuming and make them unavailable to other subscribers.

   If ack is false,
   it will not ack the message."
  [ack? queue-name count]
  (remove nil?
          (with-open [ch (lch/open connection)]
            (doall (for [_ (range count)]
                     (try-consuming-dead-set-messages ch ack? queue-name))))))

(defn get-dead-set-messages-for-topic [ack? topic count]
  (get-dead-set-messages* ack?
                          (prefixed-queue-name topic
                                               (get-in-config [:rabbit-mq :dead-letter :queue-name]))
                          count))

(defn get-dead-set-messages-for-channel [ack? topic channel count]
  (get-dead-set-messages* ack?
                          (prefixed-channel-name topic channel (get-in-config [:rabbit-mq :dead-letter :queue-name]))
                          count))

(defn- message-handler [wrapped-mapper-fn]
  (fn [ch meta ^bytes payload]
    (if-let [message (convert-and-ack-message ch meta payload true)]
      (wrapped-mapper-fn message))))

(defn close [^Channel channel]
  (try
    (.close channel)
    (catch AlreadyClosedException _
      nil)))

(defn- start-subscriber* [ch prefetch-count queue-name wrapped-mapper-fn]
  (lb/qos ch prefetch-count)
  (let [consumer-tag (lcons/subscribe ch
                                      queue-name
                                      (message-handler wrapped-mapper-fn)
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                                       :handle-consume-ok-fn      (fn [consumer_tag]
                                                                    (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))})]))

(defn start-retry-subscriber* [mapper-fn topic-entity channels]
  (when (get-in-config [:retry :enabled])
    (dotimes [_ (get-in-config [:jobs :instant :worker-count])]
      (start-subscriber* (lch/open connection)
                         (get-in-config [:jobs :instant :prefetch-count])
                         (prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :instant :queue-name]))
                         (mpr/mapper-func mapper-fn topic-entity channels)))))

(defn start-channels-subscriber [channels topic-entity]
  (doseq [channel channels]
    (let [channel-key        (first channel)
          channel-handler-fn (second channel)]
      (dotimes [_ (get-in-config [:stream-router topic-entity :channels channel-key :worker-count])]
        (start-subscriber* (lch/open connection)
                           1
                           (prefixed-channel-name topic-entity channel-key (get-in-config [:rabbit-mq :instant :queue-name]))
                           (mpr/channel-mapper-func channel-handler-fn topic-entity channel-key))))))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [stream-routes]
  (doseq [stream-route stream-routes]
    (let [topic-entity  (first stream-route)
          topic-handler (-> stream-route second :handler-fn)
          channels      (-> stream-route second (dissoc :handler-fn))]
      (start-channels-subscriber channels topic-entity)
      (start-retry-subscriber* topic-handler topic-entity (keys channels)))))
