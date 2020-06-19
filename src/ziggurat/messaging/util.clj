(ns ziggurat.messaging.util
  (:require [ziggurat.channel :refer [get-keys-for-topic]]))

(defn is-connection-required? [ziggurat-config stream-routes]
  (let [all-channels (reduce (fn [all-channel-vec [topic-entity _]]
                               (concat all-channel-vec (get-keys-for-topic stream-routes topic-entity)))
                             []
                             stream-routes)]
    (or (pos? (count all-channels))
        (-> ziggurat-config :retry :enabled))))

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
