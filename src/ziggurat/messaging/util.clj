(ns ziggurat.messaging.util)

(defn get-value-with-prefix-topic [topic-entity value]
  (str (name topic-entity) "_" value))
