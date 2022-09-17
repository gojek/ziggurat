(ns ziggurat.messaging.consumer
  (:require [clojure.tools.logging :as log]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [mount.core :refer [defstate]]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [get-in-config rabbitmq-config]]
            [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.mapper :as mpr]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.producer-connection :refer [producer-connection]]
            [ziggurat.messaging.consumer-connection :refer [consumer-connection]]
            [ziggurat.messaging.util :as util]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.error :refer [report-error]]
            [cambium.core :as clog]))

(def DEFAULT_CHANNEL_PREFETCH_COUNT 20)

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn- publish-serialized-payload-to-dead-set-and-ack
  [ch delivery-tag payload topic-entity ziggurat-channel-key]
  (if (nil? ziggurat-channel-key)
    (producer/publish-to-dead-queue payload topic-entity true)
    (producer/publish-to-channel-dead-queue ziggurat-channel-key payload topic-entity true))
  (ack-message ch delivery-tag))

(defn convert-and-ack-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and converts it to `MessagePayload`. Acks the message
  if `ack?` is true."
  [ch {:keys [delivery-tag]} ^bytes payload ack? topic-entity ziggurat-channel-key]
  (try
    (let [message (nippy/thaw payload)]
      (when ack?
        (lb/ack ch delivery-tag))
      message)
    (catch Exception e
      (report-error e "Error while decoding message, publishing to dead queue...")
      (metrics/increment-count ["rabbitmq-message" "conversion"] "failure" {:topic_name (name topic-entity)})
      (publish-serialized-payload-to-dead-set-and-ack ch delivery-tag payload topic-entity ziggurat-channel-key)
      nil)))

(defn process-message-from-queue [ch meta payload topic-entity processing-fn ziggurat-channel-key]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (convert-and-ack-message ch meta payload false topic-entity ziggurat-channel-key)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling processor-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (processing-fn message-payload)
        (ack-message ch delivery-tag)
        (catch Exception e
          (report-error e "Error while processing message-payload from RabbitMQ")
          (metrics/increment-count ["rabbitmq-message" "process"] "failure" {:topic_name (name topic-entity)})
          (publish-serialized-payload-to-dead-set-and-ack ch delivery-tag payload topic-entity ziggurat-channel-key))))))

(defn read-message-from-queue [ch queue-name topic-entity ack? ziggurat-channel-key]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (convert-and-ack-message ch meta payload ack? topic-entity ziggurat-channel-key)))
    (catch Exception e
      (report-error e "Error while consuming the dead set message")
      (metrics/increment-count ["rabbitmq-message" "consumption"] "failure" {:topic_name (name topic-entity)}))))

(defn- construct-queue-name
  ([topic-entity]
   (construct-queue-name topic-entity nil))
  ([topic-entity channel]
   (if (nil? channel)
     (util/prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :dead-letter :queue-name]))
     (util/prefixed-channel-name topic-entity channel (get-in-config [:rabbit-mq :dead-letter :queue-name])))))

(defn get-dead-set-messages
  "This method can be used to read and optionally ack messages in dead-letter queue, based on the value of `ack?`.

   For example, this method can be used to delete messages from dead-letter queue if `ack?` is set to true."
  ([topic-entity count]
   (get-dead-set-messages topic-entity nil count))
  ([topic-entity channel count]
   (remove nil?
           (with-open [ch (lch/open consumer-connection)]
             (doall (for [_ (range count)]
                      (read-message-from-queue ch (construct-queue-name topic-entity channel) topic-entity false channel)))))))

(defn process-dead-set-messages
  "This method reads and processes `count` number of messages from RabbitMQ dead-letter queue for topic `topic-entity` and
   channel specified by `channel`. Executes `processing-fn` for every message read from the queue."
  ([topic-entity count processing-fn]
   (process-dead-set-messages topic-entity nil count processing-fn))
  ([topic-entity channel count processing-fn]
   (with-open [ch (lch/open consumer-connection)]
     (doall (for [_ (range count)]
              (let [queue-name (construct-queue-name topic-entity channel)
                    [meta payload] (lb/get ch queue-name false)]
                (when (some? payload)
                  (process-message-from-queue ch meta payload topic-entity processing-fn channel))))))))

(defn delete-dead-set-messages
  "This method deletes `count` number of messages from RabbitMQ dead-letter queue for topic `topic-entity` and channel
  `channel`."
  [topic-entity channel count]
  (with-open [ch (lch/open producer-connection)]
    (let [queue-name (construct-queue-name topic-entity channel)]
      (doall (for [_ (range count)]
               (lb/get ch queue-name true))))))

(defn- message-handler [wrapped-mapper-fn topic-entity ziggurat-channel-key]
  (fn [ch meta ^bytes payload]
    (clog/with-logging-context {:consumer-group topic-entity} (process-message-from-queue ch meta payload topic-entity wrapped-mapper-fn ziggurat-channel-key))))

(defn- start-subscriber* [prefetch-count queue-name wrapped-mapper-fn topic-entity ziggurat-channel-key]
  "
  Returns a map containing subscriber info. The channel used to subscribe and the consumer-tag
  "
  (let [ch (lch/open consumer-connection)]
    (lb/qos ch prefetch-count)
    (let [consumer-tag (lcons/subscribe ch
                                        queue-name
                                        (message-handler wrapped-mapper-fn topic-entity ziggurat-channel-key)
                                        {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                      (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                                         :handle-consume-ok-fn      (fn [consumer_tag]
                                                                      (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))})]
      {:rabbitmq-channel ch :consumer-tag consumer-tag})))

(defn start-retry-subscriber* [handler-fn topic-entity]
  (when (get-in-config [:retry :enabled])
    (reduce (fn [subscribers _]
              "
              subscriber is a map contains keys :rabbitmq-channel and :consumer-tag
              "
              (let [subscriber (start-subscriber* (get-in-config [:jobs :instant :prefetch-count])
                                                  (util/prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :instant :queue-name]))
                                                  handler-fn
                                                  topic-entity
                                                  nil)]
                (conj subscribers subscriber))) [] (range (get-in-config [:jobs :instant :worker-count])))))

(defn start-channels-subscriber [channels topic-entity]
  "
   Returns the following map
   {
     :topic-entity -> {
          :ziggurat-channel-1 -> [
              {
                   :rabbitmq-channel <channel-object>
                   :consumer-tag  'string'
              },
              { :rabbitmq-channel <channel-object-2>
                :consumer-tag  'string'
              }
          ],
          :ziggurat-channel-2 -> [
            {
              :rabbitmq-channel <channel-object>
              :consumer-tag  'string'
            }
          ]
     }
   }
  "

  (reduce (fn [channel-consumer-tags-map channel]
            (let [channel-key (first channel)
                  channel-handler-fn (second channel)
                  subscribers-for-channel (reduce (fn [subscribers _]
                                                    (let [channel-prefetch-count (get-in-config [:stream-router topic-entity :channels channel-key :prefetch-count] DEFAULT_CHANNEL_PREFETCH_COUNT)
                                                          subscriber (start-subscriber*   channel-prefetch-count
                                                                                          (util/prefixed-channel-name topic-entity channel-key (get-in-config [:rabbit-mq :instant :queue-name]))
                                                                                          (mpr/channel-mapper-func channel-handler-fn channel-key)
                                                                                          topic-entity
                                                                                          channel-key)]
                                                      (conj subscribers subscriber))) [] (range (get-in-config [:stream-router topic-entity :channels channel-key :worker-count])))]
              (assoc channel-consumer-tags-map channel-key subscribers-for-channel)))
          {} channels))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [stream-routes batch-routes]
  (if (not (nil? consumer-connection))
    (let [stream-consumers (reduce (fn [subscriber-map stream-route]
                                     (let [topic-entity (first stream-route)
                                           handler (-> stream-route second :handler-fn)
                                           channels (-> stream-route second (dissoc :handler-fn))
                                           channel-subscribers (start-channels-subscriber channels topic-entity)
                                           retry-subscribers (start-retry-subscriber* (mpr/mapper-func handler (keys channels)) topic-entity)]
                                       (assoc subscriber-map topic-entity {:retry retry-subscribers :channels channel-subscribers})))
                                   {} stream-routes)

          batch-consumers (reduce (fn [subscriber-map batch-route]
                                    (let [topic-entity (first batch-route)
                                          handler (-> batch-route second :handler-fn)
                                          retry-subscribers (start-retry-subscriber* (fn [message] (ch/process handler message)) topic-entity)]
                                      (assoc subscriber-map topic-entity {:retry retry-subscribers}))) {} batch-routes)
          data {:stream-consumers stream-consumers :batch-consumers batch-consumers}
          _ (log/info "Subscriber info" data)]
      data)))

(defn stop-subscribers [subscribers additional-message]
  (doseq [{:keys [rabbitmq-channel consumer-tag]} subscribers]
    (log/infof "Stopping consumer with tag %s for %s", consumer-tag, additional-message)
    (lb/cancel rabbitmq-channel consumer-tag)))

(defn stop-subscribers-for-consumers [consumers]
  (doseq [topic-entity-data consumers]
    (let [topic-entity (first topic-entity-data)
          consumer-map (second topic-entity-data)]
      (when (contains? consumer-map :retry)
        (stop-subscribers (:retry consumer-map) (format "topic-entity %s", topic-entity)))

      (when (contains? consumer-map :channels)
        (doseq [channel-subscribers (:channels consumer-map)]
          (let [channel (first channel-subscribers)
                subscribers (second channel-subscribers)]
            (stop-subscribers subscribers (format "topic-entity %s, channel %s", topic-entity, channel))))))))

(declare consumers)

(defstate consumers
  "consumers is a map containing channel and consumer tags for stream and batch routes. One channel is used
   for all the subscribers and is preserved until shutdown. We have to preserve the
   channel because for cancelling a subscription, rabbitmq requires the same channel
   that created the consumer tag for the subscription."
  :start (do
           (log/info "Starting rabbitmq consumers")
           (start-subscribers (:stream-routes (mount.core/args)) (:batch-routes (mount.core/args))))
  :stop (do
          (log/info "Stopping rabbitmq consumers")
          (when (map? consumers)
            (when (contains? consumers :stream-consumers)
              (stop-subscribers-for-consumers (:stream-consumers consumers)))
            (when (contains? consumers :batch-consumers)
              (stop-subscribers-for-consumers (:batch-consumers consumers))))))

