(ns ziggurat.messaging.dead-set
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay
  "Gets the message from the queue and puts them to instant queue"
  [count-of-message topic-entity channel]
  (if (nil? channel)
    (doseq [msg (consumer/get-dead-set-messages-for-topic true topic-entity count-of-message)]
      (producer/publish-to-instant-queue topic-entity msg))
    (doseq [msg (consumer/get-dead-set-messages-for-channel true topic-entity channel count-of-message)]
      (producer/publish-to-channel-instant-queue topic-entity channel msg))))

(defn view
  "Gets n number of messages from dead queue"
  [count-of-message topic-entity channel]
  (if (nil? channel)
    (consumer/get-dead-set-messages-for-channel false topic-entity channel count-of-message)
    (consumer/get-dead-set-messages-for-topic false topic-entity count-of-message)))