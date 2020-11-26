(ns ziggurat.messaging.util
  (:require [clojure.string :as str]))

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
  (let [{:keys [host hosts]} config]
    (if hosts
      (str/split hosts #",")
      [host])))
