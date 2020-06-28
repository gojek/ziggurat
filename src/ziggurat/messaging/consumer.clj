(ns ziggurat.messaging.consumer
  (:require [ziggurat.mapper :as mpr]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.config :refer [ziggurat-config get-in-config]]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.messaging.util :refer :all]
            [ziggurat.metrics :as metrics]
            [clojure.tools.logging :as log]
            [schema.core :as s]))

(defn get-dead-set-queue-name
  ([topic-entity ziggurat-config]
   (get-dead-set-queue-name topic-entity ziggurat-config nil))
  ([topic-entity ziggurat-config channel]
   (if (nil? channel)
     (prefixed-queue-name topic-entity (get-in ziggurat-config [:rabbit-mq :dead-letter :queue-name]))
     (prefixed-channel-name topic-entity channel (get-in ziggurat-config [:rabbit-mq :dead-letter :queue-name])))))

(defn- get-instant-queue-name [topic-entity ziggurat-config]
  (let [instant-queue-name (get-in ziggurat-config [:rabbit-mq :instant :queue-name])]
    (prefixed-queue-name topic-entity instant-queue-name)))

(defn- get-channel-instant-queue-name [topic-entity channel-key ziggurat-config]
  (let [instant-queue-name (get-in ziggurat-config [:rabbit-mq :instant :queue-name])]
    (prefixed-channel-name topic-entity channel-key instant-queue-name)))

(defn convert-to-message-payload
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

(defn read-messages-from-queue [queue-name topic-entity ack? count]
  (let [messages (messaging/get-messages-from-queue queue-name ack? count)]
    (for [message messages]
      (if-not (nil? message)
        (convert-to-message-payload message topic-entity)
        (metrics/increment-count ["rabbitmq-message" "consumption"] "failure" {:topic_name (name topic-entity)})))))

(defn get-dead-set-messages
  "This method can be used to read and optionally ack messages in dead-letter queue, based on the value of `ack?`.
   For example, this method can be used to delete messages from dead-letter queue if `ack?` is set to true."
  ([topic-entity count]
   (get-dead-set-messages topic-entity nil count))
  ([topic-entity channel count]
   (remove nil?
           (read-messages-from-queue (get-dead-set-queue-name topic-entity (ziggurat-config) channel) topic-entity false count))))

(defn process-messages-from-queue [queue-name count processing-fn]
  (messaging/process-messages-from-queue queue-name count processing-fn))

(defn process-dead-set-messages
  "This method reads and processes `count` number of messages from RabbitMQ dead-letter queue for topic `topic-entity` and
   channel specified by `channel`. Executes `processing-fn` for every message read from the queue."
  ([topic-entity count processing-fn]
   (process-dead-set-messages topic-entity nil count processing-fn))
  ([topic-entity channel count processing-fn]
   (process-messages-from-queue (get-dead-set-queue-name topic-entity (ziggurat-config) channel) count processing-fn)))

(defn start-retry-subscriber* [mapper-fn topic-entity channels ziggurat-config]
  (when (get-in ziggurat-config [:retry :enabled])
    (dotimes [_ (get-in ziggurat-config [:jobs :instant :worker-count])]
      (let [queue-name (get-instant-queue-name topic-entity ziggurat-config)]
        (messaging/start-subscriber (get-in ziggurat-config [:jobs :instant :prefetch-count])
                                    (mpr/mapper-func mapper-fn channels)
                                    queue-name)))))

(defn start-channels-subscriber [channels topic-entity ziggurat-config]
  (doseq [channel channels]
    (let [channel-key        (first channel)
          channel-handler-fn (second channel)]
      (dotimes [_ (get-in-config [:stream-router topic-entity :channels channel-key :worker-count] 0)]
        (let [queue-name (get-channel-instant-queue-name topic-entity channel-key ziggurat-config)]
          (messaging/start-subscriber 1
                                      (mpr/channel-mapper-func channel-handler-fn channel-key)
                                      queue-name))))))

(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [stream-routes ziggurat-config]
  (doseq [stream-route stream-routes]
    (let [topic-entity  (first stream-route)
          topic-handler (-> stream-route second :handler-fn)
          channels      (-> stream-route second (dissoc :handler-fn))]
      (start-channels-subscriber channels topic-entity ziggurat-config)
      (start-retry-subscriber* topic-handler topic-entity (keys channels) ziggurat-config))))
