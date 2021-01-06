(ns ziggurat.middleware.batch.batch-proto-deserializer
  (:require [ziggurat.middleware.default :refer [deserialize-message]]))

(defn- deserialize-key-and-value
  [key-proto-class value-proto-class topic-entity flatten-protobuf-struct?]
  (fn [message]
    (let [key                (:key message)
          value              (:value message)
          deserialized-key   (when (some? key) (deserialize-message key key-proto-class (name topic-entity) flatten-protobuf-struct?))
          deserialized-value (when (some? value) (deserialize-message value value-proto-class (name topic-entity) flatten-protobuf-struct?))]
      (assoc (assoc message :key deserialized-key) :value deserialized-value))))

(defn deserialize-batch-of-proto-messages
  "This is a middleware function that takes in a sequence of proto message and calls forms a lazy sequence of
   de-serialized messages before passing it to the handler-fn"
  ([handler-fn key-proto-class value-proto-class topic-entity]
   (deserialize-batch-of-proto-messages handler-fn key-proto-class value-proto-class topic-entity false))
  ([handler-fn key-proto-class value-proto-class topic-entity flatten-protobuf-struct?]
   (fn [batch-message]
     (let [key-value-deserializer (deserialize-key-and-value key-proto-class value-proto-class topic-entity flatten-protobuf-struct?)]
       (handler-fn (map key-value-deserializer batch-message))))))
