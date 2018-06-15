(ns ziggurat.messaging.util)

(defn get-value-with-prefix-topic [topic-entity value]
  (str topic-entity "_" value))