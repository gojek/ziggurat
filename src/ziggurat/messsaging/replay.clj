(ns ziggurat.messsaging.replay
  (:require [ziggurat.messsaging.consumer :as consumer]
            [ziggurat.messsaging.producer :as producer]))

(defn replay [count-of-message]
  (-> count-of-message
      (consumer/get-dead-set-messages)
      (producer/publish-to-instant-queue)))
