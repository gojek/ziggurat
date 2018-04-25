(ns ziggurat.messaging.dead-set
  (:require [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))

(defn replay [count-of-message]
  (-> count-of-message
      (consumer/get-dead-set-messages true)
      (producer/publish-to-instant-queue)))

(defn view [count-of-message]
  (-> count-of-message
      (consumer/get-dead-set-messages false)
      (producer/publish-to-instant-queue)))
