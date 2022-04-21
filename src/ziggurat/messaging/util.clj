(ns ziggurat.messaging.util
  (:require [clojure.string :as str])
  (:import (com.rabbitmq.client DnsRecordIpAddressResolver ListAddressResolver Address)))

(defn prefixed-queue-name [topic-entity value]
  (str (name topic-entity) "_" value))

(defn with-channel-name [topic-entity channel]
  (str (name topic-entity) "_channel_" (name channel)))

(defn prefixed-channel-name [topic-entity channel-name value]
  (prefixed-queue-name (with-channel-name topic-entity channel-name)
                       value))

(defn get-channel-names [stream-routes topic-entity]
  (-> stream-routes
      (get topic-entity)
      (dissoc :handler-fn)
      keys))

(defn list-of-hosts [config]
  (let [{:keys [host hosts]} config
        rabbitmq-hosts       (if (some? hosts)
                               hosts
                               host)]
    (str/split rabbitmq-hosts #",")))

(defn create-address-resolver
  [rabbitmq-config]
  (let [host (:hosts rabbitmq-config)
        port (:port rabbitmq-config)
        address-resolver (get rabbitmq-config :address-resolver :dns)]
    (if (= address-resolver :dns)
      (DnsRecordIpAddressResolver. ^String host ^int port)
      (ListAddressResolver. (map #(Address. %) (list-of-hosts rabbitmq-config))))))
