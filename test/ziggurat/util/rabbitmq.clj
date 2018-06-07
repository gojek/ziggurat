(ns ziggurat.util.rabbitmq
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [ziggurat.config :refer [rabbitmq-config]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]))


(defn- get-msg-from-rabbitmq [queue-name]
  (with-open [ch (lch/open connection)]
    (let [[meta payload] (lb/get ch queue-name false)]
      (consumer/convert-and-ack-message ch meta payload true))))

(defn get-msg-from-delay-queue [topic-name]
  (let [{:keys [queue-name queue-timeout-ms]} (:delay (rabbitmq-config))
        queue-name (producer/delay-queue-name topic-name queue-name queue-timeout-ms)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-msg-from-dead-queue [topic-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-msg-from-instant-queue [topic-name]
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name)))
