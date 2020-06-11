(ns ziggurat.messaging.consumer
  (:require [langohr.basic :as lb]
            [langohr.channel :as lch]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [get-in-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.util :refer :all]
            [ziggurat.metrics :as metrics]))


(defn read-message-from-queue [ch queue-name topic-entity ack?]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (rmqw/convert-and-ack-message ch meta payload ack? topic-entity)))
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
                  (rmqw/process-message-from-queue ch meta payload topic-entity processing-fn))))))))

(defn start-retry-subscriber* [mapper-fn topic-entity channels]
  (when (get-in-config [:retry :enabled])
    (dotimes [_ (get-in-config [:jobs :instant :worker-count])]
      (rmqw/start-subscriber (lch/open connection)
                         (get-in-config [:jobs :instant :prefetch-count])
                         (prefixed-queue-name topic-entity (get-in-config [:rabbit-mq :instant :queue-name]))
                         (mpr/mapper-func mapper-fn channels)
                         topic-entity))))

(defn start-channels-subscriber [channels topic-entity]
  (doseq [channel channels]
    (let [channel-key        (first channel)
          channel-handler-fn (second channel)]
      (dotimes [_ (get-in-config [:stream-router topic-entity :channels channel-key :worker-count])]
        (rmqw/start-subscriber (lch/open connection)
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
