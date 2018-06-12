(ns ziggurat.messaging.util)

(defn get-name-with-prefix-topic [topic-name name]
  (if (nil? topic-name)
    name
    (str topic-name "_" name)))
