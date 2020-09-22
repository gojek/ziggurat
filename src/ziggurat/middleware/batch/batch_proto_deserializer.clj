(ns ziggurat.middleware.batch.batch-proto-deserializer
  (:require [ziggurat.middleware.default :refer [deserialize-message]]))

(defn deserialize-batch-of-proto-messages
  "This is a middleware function that takes in a sequence of proto message and calls forms a lazy sequence of
   de-serialized messages before passing it to the handler-fn"
  [handler-fn proto-class topic-entity-name]
  (fn [batch-message]
    (handler-fn (map #(deserialize-message % proto-class topic-entity-name) batch-message))))
