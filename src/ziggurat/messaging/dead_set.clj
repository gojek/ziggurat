(ns ziggurat.messaging.dead-set
  (:require [langohr.basic :as lb]
            [langohr.channel :as lch]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [get-in-config]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :refer :all]))

(defn- try-consuming-dead-set-messages [ch ack? queue-name topic-entity]
  (try
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (some? payload)
        (consumer/convert-and-ack-message ch meta payload ack? topic-entity)))
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while consuming the dead set message"))))

(defn- get-dead-set-messages*
  "Get the n(count) messages from the rabbitmq.

   If ack is set to true,
   then ack all the messages while consuming and make them unavailable to other subscribers.

   If ack is false,
   it will not ack the message."
  [ack? queue-name count topic-entity]
  (remove nil?
          (with-open [ch (lch/open connection)]
            (doall (for [_ (range count)]
                     (try-consuming-dead-set-messages ch ack? queue-name topic-entity))))))

(defn get-dead-set-messages-for-topic [ack? topic-entity count]
  (get-dead-set-messages* ack?
                          (prefixed-queue-name topic-entity
                                               (get-in-config [:rabbit-mq :dead-letter :queue-name]))
                          count
                          topic-entity))

(defn get-dead-set-messages-for-channel [ack? topic-entity channel count]
  (get-dead-set-messages* ack?
                          (prefixed-channel-name topic-entity channel (get-in-config [:rabbit-mq :dead-letter :queue-name]))
                          count
                          topic-entity))

(defn replay
  "Gets the message from the queue and puts them to instant queue"
  [count-of-message topic-entity channel]
  (if (nil? channel)
    (doseq [message-payload (get-dead-set-messages-for-topic true topic-entity count-of-message)]
      (producer/publish-to-instant-queue message-payload))
    (doseq [message-payload (get-dead-set-messages-for-channel true topic-entity channel count-of-message)]
      (producer/publish-to-channel-instant-queue channel message-payload))))

(defn- get-messages
  "Gets n messages from dead queue and gives the option to ack or un-ack them"
  [count-of-message topic-entity channel ack?]
  (if (nil? channel)
    (get-dead-set-messages-for-topic ack? topic-entity count-of-message)
    (get-dead-set-messages-for-channel ack? topic-entity channel count-of-message)))

(defn view
  "Gets n number of messages from dead queue"
  [count-of-message topic-entity channel]
  (get-messages count-of-message topic-entity channel false))

(defn delete
  "Deletes n number of messages from dead queue"
  [count-of-message topic-entity channel]
  (get-messages count-of-message topic-entity channel true))

