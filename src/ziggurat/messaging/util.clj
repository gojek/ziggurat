(ns ziggurat.messaging.util)

(defn get-name-with-prefix-topic [topic-entity name]
  (if (nil? topic-entity)
    name
    (str topic-entity "_" name)))
