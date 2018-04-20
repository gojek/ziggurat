(ns ziggurat.messaging.replay
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay [count-of-message]
  (-> count-of-message
      (consumer/get-dead-set-messages)
      (producer/publish-to-instant-queue)))
