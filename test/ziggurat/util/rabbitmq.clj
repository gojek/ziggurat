(ns ziggurat.util.rabbitmq
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [ziggurat.config :refer [rabbitmq-config]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.util :refer [prefixed-channel-name]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :as rutil]))

(defn- get-msg-from-rabbitmq [queue-name]
  (with-open [ch (lch/open connection)]
    (let [[meta payload] (lb/get ch queue-name false)]
      (when (seq payload)
        (consumer/convert-and-ack-message ch meta payload true)))))

(defn get-msg-from-delay-queue [topic-name]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        queue-name (producer/delay-queue-name topic-name queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-msg-from-dead-queue [topic-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-msg-from-channel-dead-queue [topic-name channel-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (prefixed-channel-name topic-name channel-name queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-msg-from-instant-queue [topic-name]
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-message-from-channel-delay-queue [topic channel]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        queue-name (producer/delay-queue-name (rutil/with-channel-name topic channel) queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-message-from-channel-instant-queue [topic-name channel-name]
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))
        queue-name (prefixed-channel-name topic-name channel-name queue-name)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-message-from-retry-queue [topic sequence]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        delay-queue-name (producer/delay-queue-name topic queue-name)
        queue-name (rutil/prefixed-queue-name delay-queue-name sequence)]
    (get-msg-from-rabbitmq queue-name)))

(defn get-message-from-channel-retry-queue [topic channel sequence]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        delay-queue-name (producer/delay-queue-name (rutil/with-channel-name topic channel) queue-name)
        queue-name (rutil/prefixed-queue-name delay-queue-name sequence)]
    (get-msg-from-rabbitmq queue-name)))
