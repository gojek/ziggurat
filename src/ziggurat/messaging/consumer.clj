(ns ziggurat.messaging.consumer
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [get-in-config rabbitmq-config ziggurat-config]]
            [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.mapper :as mpr]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.util :as util]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.error :refer [report-error]]
            [cambium.core :as clog]))

(defn- reject-message
  [ch delivery-tag]
  (lb/reject ch delivery-tag))

(defn- publish-to-dead-set
  [ch delivery-tag topic-entity payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        exchange                (util/prefixed-queue-name topic-entity exchange-name)
        tags {:topic-entity (name topic-entity)
              :exchange (str/replace exchange (:app-name (ziggurat-config)) "app-name")}]
    (try
      (lb/publish ch exchange "" payload)
      (metrics/prom-inc :ziggurat/rabbitmq-publish-count tags)
      (catch Exception e
        (log/error e "Exception was encountered while publishing to RabbitMQ")
        (metrics/prom-inc :ziggurat/rabbitmq-publish-failure-count tags)
        (reject-message ch delivery-tag)))))

(defn convert-and-ack-message
  "De-serializes the message payload (`payload`) using `nippy/thaw` and converts it to `MessagePayload`. Acks the message
  if `ack?` is true."
  [ch {:keys [delivery-tag]} ^bytes payload ack? topic-entity queue-name]
  (try
    (let [message (nippy/thaw payload)]
      (when ack?
        (lb/ack ch delivery-tag))
      (metrics/prom-inc :ziggurat/rabbitmq-read-count {:topic_name (name topic-entity) :queue (str/replace queue-name (:app-name (ziggurat-config)) "app-name")})
      message)
    (catch Exception e
      (report-error e "Error while decoding message, publishing to dead queue...")
      (publish-to-dead-set ch delivery-tag topic-entity payload)
      (metrics/increment-count ["rabbitmq-message" "conversion"] "failure" {:topic_name (name topic-entity)})
      (metrics/prom-inc :ziggurat/rabbitmq-read-failure-count {:topic_name (name topic-entity) :queue queue-name})
      nil)))

(defn- ack-message
  [ch delivery-tag]
  (lb/ack ch delivery-tag))

(defn process-message-from-queue [ch meta payload topic-entity processing-fn queue-name]
  (let [delivery-tag    (:delivery-tag meta)
        message-payload (convert-and-ack-message ch meta payload false topic-entity queue-name)]
    (when message-payload
      (log/infof "Processing message [%s] from RabbitMQ " message-payload)
      (try
        (log/debug "Calling processor-fn with the message-payload - " message-payload " with retry count - " (:retry-count message-payload))
        (processing-fn message-payload)
        (ack-message ch delivery-tag)
        (catch Exception e
          (publish-to-dead-set ch delivery-tag topic-entity payload)
          (report-error e "Error while processing message-payload from RabbitMQ")
          (metrics/increment-count ["rabbitmq-message" "process"] "failure" {:topic_name (name topic-entity)}))))))

(defn read-message-from-queue [ch queue-name topic-entity ack?]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (convert-and-ack-message ch meta payload ack? topic-entity queue-name)))
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
                  (process-message-from-queue ch meta payload topic-entity processing-fn queue-name))))))))

(defn- message-handler [wrapped-mapper-fn topic-entity queue-name]
  (fn [ch meta ^bytes payload]
    (clog/with-logging-context {:consumer-group topic-entity} (process-message-from-queue ch meta payload topic-entity wrapped-mapper-fn queue-name))))

(defn- start-subscriber* [ch prefetch-count queue-name wrapped-mapper-fn topic-entity]
  (lb/qos ch prefetch-count)
  (lcons/subscribe ch
                   queue-name
                   (message-handler wrapped-mapper-fn topic-entity queue-name)
                   {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                 (log/infof "channel closed with consumer tag: %s, reason: %s " consumer_tag, reason))
                    :handle-consume-ok-fn      (fn [consumer_tag]
                                                 (log/infof "consumer started for %s with consumer tag %s " queue-name consumer_tag))}))

(defn start-retry-subscriber* [handler-fn topic-entity]
  (when (get-in-config [:retry :enabled])
    (dotimes [_ (get-in-config [:jobs :instant :worker-count])]
      (start-subscriber* (lch/open connection)
                         (get-in-config [:jobs :instant :prefetch-count])
                         (util/prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :instant :queue-name]))
                         handler-fn
                         topic-entity))))

(defn start-channels-subscriber [channels topic-entity]
  (doseq [channel channels]
    (let [channel-key        (first channel)
          channel-handler-fn (second channel)]
      (dotimes [_ (get-in-config [:stream-router topic-entity :channels channel-key :worker-count])]
        (start-subscriber* (lch/open connection)
                           1
                           (util/prefixed-channel-name topic-entity channel-key (get-in-config [:rabbit-mq :instant :queue-name]))
                           (mpr/channel-mapper-func channel-handler-fn channel-key)
                           topic-entity)))))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [stream-routes batch-routes]
  (doseq [stream-route stream-routes]
    (let [topic-entity (first stream-route)
          handler      (-> stream-route second :handler-fn)
          channels     (-> stream-route second (dissoc :handler-fn))]
      (start-channels-subscriber channels topic-entity)
      (start-retry-subscriber* (mpr/mapper-func handler (keys channels)) topic-entity)))
  (doseq [batch-route batch-routes]
    (let [topic-entity (first batch-route)
          handler      (-> batch-route second :handler-fn)]
      (start-retry-subscriber* (fn [message] (ch/process handler message)) topic-entity))))
