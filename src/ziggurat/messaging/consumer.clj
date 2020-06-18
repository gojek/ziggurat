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
            [ziggurat.messaging.util :refer [prefixed-channel-name prefixed-queue-name]]
            [ziggurat.metrics :as metrics]))

(defn- convert-to-message-payload
  "This function is used for migration from Ziggurat Version 2.x to 3.x. It checks if the message is a message payload or a message(pushed by Ziggurat version < 3.0.0) and converts messages to
   message-payload to pass onto the mapper-fn.

   If the `:retry-count` key is absent in the `message`, then it puts `0` as the value for `:retry-count` in `MessagePayload`.
   It also converts the topic-entity into a keyword while constructing MessagePayload."
  [message topic-entity]
  (try
    (s/validate mpr/message-payload-schema message)
    (catch Exception e
      (log/info "old message format read, converting to message-payload: " message)
      (let [retry-count     (or (:retry-count message) 0)
            message-payload (mpr/->MessagePayload (dissoc message :retry-count) (keyword topic-entity))]
        (assoc message-payload :retry-count retry-count)))))

(defn convert-and-ack-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and converts it to `MessagePayload`. Acks the message
  if `ack?` is true."
  [ch {:keys [delivery-tag]} ^bytes payload ack? topic-entity]
  (try
    (let [message (nippy/thaw payload)]
      (when ack?
        (lb/ack ch delivery-tag))
      (convert-to-message-payload message topic-entity))
    (catch Exception e
      (lb/reject ch delivery-tag false)
      (sentry/report-error sentry-reporter e "Error while decoding message")
      (metrics/increment-count ["rabbitmq-message" "conversion"] "failure" {:topic_name (name topic-entity)})
      nil)))

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn process-message-from-queue [ch meta payload topic-entity processing-fn]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (convert-and-ack-message ch meta payload false topic-entity)]
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

(defn read-message-from-queue [ch queue-name topic-entity ack?]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (convert-and-ack-message ch meta payload ack? topic-entity)))
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while consuming the dead set message")
      (metrics/increment-count ["rabbitmq-message" "consumption"] "failure" {:topic_name (name topic-entity)}))))

(defn- construct-queue-name
  ([topic-entity]
   (construct-queue-name topic-entity nil))
  ([topic-entity channel]
   (if (nil? channel)
     (prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :dead-letter :queue-name]))
     (prefixed-channel-name topic-entity channel (get-in-config [:rabbit-mq :dead-letter :queue-name])))))

(defn get-dead-set-messages
  "This method can be used to read and optionally ack messages in dead-letter queue, based on the value of `ack?`.

   For example, this method can be used to delete messages from dead-letter queue if `ack?` is set to true."
  ([topic-entity count]
   (get-dead-set-messages topic-entity nil count))
  ([topic-entity channel count]
   (remove nil?
           (with-open [ch (lch/open connection)]
             (doall (for [_ (range count)]
                      (read-message-from-queue ch (construct-queue-name topic-entity channel) topic-entity false)))))))

(defn process-dead-set-messages
  "This method reads and processes `count` number of messages from RabbitMQ dead-letter queue for topic `topic-entity` and
   channel specified by `channel`. Executes `processing-fn` for every message read from the queue."
  ([topic-entity count processing-fn]
   (process-dead-set-messages topic-entity nil count processing-fn))
  ([topic-entity channel count processing-fn]
   (with-open [ch (lch/open connection)]
     (doall (for [_ (range count)]
              (let [queue-name     (construct-queue-name topic-entity channel)
                    [meta payload] (lb/get ch queue-name false)]
                (when (some? payload)
                  (process-message-from-queue ch meta payload topic-entity processing-fn))))))))

(defn- message-handler [wrapped-mapper-fn topic-entity]
  (fn [ch meta ^bytes payload]
    (process-message-from-queue ch meta payload topic-entity wrapped-mapper-fn)))

(defn- start-subscriber* [ch prefetch-count queue-name wrapped-mapper-fn topic-entity]
  (lb/qos ch prefetch-count)
  (lcons/subscribe ch
                   queue-name
                   (message-handler wrapped-mapper-fn topic-entity)
                   {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                 (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                    :handle-consume-ok-fn      (fn [consumer_tag]
                                                 (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))}))

(defn start-retry-subscriber* [mapper-fn topic-entity channels]
  (when (get-in-config [:retry :enabled])
    (dotimes [_ (get-in-config [:jobs :instant :worker-count])]
      (start-subscriber* (lch/open connection)
                         (get-in-config [:jobs :instant :prefetch-count])
                         (prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :instant :queue-name]))
                         (mpr/mapper-func mapper-fn channels)
                         topic-entity))))

(defn start-channels-subscriber [channels topic-entity]
  (doseq [channel channels]
    (let [channel-key        (first channel)
          channel-handler-fn (second channel)
          worker-count       (get-in-config [:stream-router topic-entity :channels channel-key :worker-count] 0)]
      (dotimes [_ worker-count]
        (start-subscriber* (lch/open connection)
                           1
                           (prefixed-channel-name topic-entity channel-key (get-in-config [:rabbit-mq :instant :queue-name]))
                           (mpr/channel-mapper-func channel-handler-fn channel-key)
                           topic-entity)))))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [stream-routes]
  (doseq [stream-route stream-routes]
    (let [topic-entity  (first stream-route)
          topic-handler (-> stream-route second :handler-fn)
          channels      (-> stream-route second (dissoc :handler-fn))]
      (start-channels-subscriber channels topic-entity)
      (start-retry-subscriber* topic-handler topic-entity (keys channels)))))
