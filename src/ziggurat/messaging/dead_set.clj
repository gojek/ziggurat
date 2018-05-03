(ns ziggurat.messaging.dead-set
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay [count-of-message]
  "Gets the message from the queue and puts them to instant queue"
  (-> count-of-message
      (consumer/get-dead-set-messages true)
      (producer/publish-to-instant-queue)))

(defn view
  "Gets n number of messages from dead queue"
  [count-of-message]
  (consumer/get-dead-set-messages count-of-message false))
