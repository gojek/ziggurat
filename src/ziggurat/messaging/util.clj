(ns ziggurat.messaging.util)

(defn prefixed-queue-name [topic-entity value]
  (str (name topic-entity) "_" value))
