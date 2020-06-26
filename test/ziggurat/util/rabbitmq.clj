(ns ziggurat.util.rabbitmq
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.basic :as lb]
            [ziggurat.config :refer [rabbitmq-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw :refer [connection]]
            [ziggurat.messaging.util :refer [prefixed-channel-name]]
            [ziggurat.messaging.producer :refer [delay-queue-name]]
            [ziggurat.messaging.util :as rutil]
            [ziggurat.tracer :refer [tracer]]
            [clojure.tools.logging :as log])
  (:import (com.rabbitmq.client AlreadyClosedException Channel)))

(defn- get-msg-from-rabbitmq [queue-name topic-name]
  (with-open [ch (lch/open @connection)]
    (try
      (let [[meta payload] (lb/get ch queue-name false)]
        (when (seq payload)
          (rmqw/consume-message ch meta payload true)))
      (catch NullPointerException e
        nil))))

(defn- get-msg-from-rabbitmq-without-ack [queue-name topic-name]
  (with-open [ch (lch/open @connection)]
    (try
      (let [[meta payload] (lb/get ch queue-name false)]
        (when (seq payload)
          (rmqw/consume-message ch meta payload false)))
      (catch NullPointerException e
        nil))))

(defn get-msg-from-delay-queue [topic-name]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        queue-name (delay-queue-name topic-name queue-name)]
    (get-msg-from-rabbitmq queue-name topic-name)))

(defn get-msg-from-dead-queue [topic-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name topic-name)))

(defn get-msg-from-dead-queue-without-ack [topic-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq-without-ack queue-name topic-name)))

(defn get-msg-from-channel-dead-queue [topic-name channel-name]
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))
        queue-name (prefixed-channel-name topic-name channel-name queue-name)]
    (get-msg-from-rabbitmq queue-name topic-name)))

(defn get-msg-from-instant-queue [topic-name]
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))
        queue-name (str topic-name "_" queue-name)]
    (get-msg-from-rabbitmq queue-name topic-name)))

(defn get-message-from-channel-delay-queue [topic channel]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        queue-name (delay-queue-name (rutil/with-channel-name topic channel) queue-name)]
    (get-msg-from-rabbitmq queue-name topic)))

(defn get-message-from-channel-instant-queue [topic-name channel-name]
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))
        queue-name (prefixed-channel-name topic-name channel-name queue-name)]
    (get-msg-from-rabbitmq queue-name topic-name)))

(defn get-message-from-retry-queue [topic sequence]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        delay-queue-name (delay-queue-name topic queue-name)
        queue-name       (rutil/prefixed-queue-name delay-queue-name sequence)]
    (get-msg-from-rabbitmq queue-name topic)))

(defn get-message-from-channel-retry-queue [topic channel sequence]
  (let [{:keys [queue-name]} (:delay (rabbitmq-config))
        delay-queue-name (delay-queue-name (rutil/with-channel-name topic channel) queue-name)
        queue-name       (rutil/prefixed-queue-name delay-queue-name sequence)]
    (get-msg-from-rabbitmq queue-name topic)))

(defn close [^Channel channel]
  (try
    (.close channel)
    (catch AlreadyClosedException _
      nil)))
