(ns ziggurat.messaging.dead-set
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay
  "Gets the message from the queue and puts them to instant queue"
  [count-of-message topic-entity channel]
  (if (nil? channel)
    (consumer/process-dead-set-messages topic-entity count-of-message
                                        (fn [message-payload]
                                          (producer/publish-to-instant-queue message-payload)))
    (consumer/process-dead-set-messages topic-entity channel count-of-message
                                        (fn [message-payload]
                                          (producer/publish-to-channel-instant-queue channel message-payload)))))

(defn view
  "Gets n number of messages from dead queue"
  [count-of-message topic-entity channel]
  (consumer/get-dead-set-messages false topic-entity channel count-of-message))

(defn delete
  "Deletes n number of messages from dead queue"
  [count-of-message topic-entity channel]
  (consumer/get-dead-set-messages true topic-entity channel count-of-message))

