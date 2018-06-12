(ns ziggurat.messaging.dead-set
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay [count-of-message topic-name]
  "Gets the message from the queue and puts them to instant queue"
  (doseq [msg (consumer/get-dead-set-messages true topic-name count-of-message)]
    (producer/publish-to-instant-queue topic-name msg)))

(defn view
  "Gets n number of messages from dead queue"
  [count-of-message topic-name]
  (consumer/get-dead-set-messages false topic-name count-of-message))
