(ns ziggurat.middleware.stream-joins
  (:require [ziggurat.middleware.default :as dmw]))

(defn- deserialize-stream-joins-message
  "This function takes in the message(proto Byte Array) and the proto-class and deserializes the proto ByteArray into a
  Clojure PersistentHashMap.
  Temporary logic for migration of services to Ziggurat V3.0
    If the message is of type map, the function just returns the map as it is. In older versions of Ziggurat (< 3.0) we stored
    the messages in deserialized formats in RabbitMQ and those messages can be processed by this function. So we have this logic here."
  [message proto-class topic-entity-name]
  (reduce
   (fn [[k1 v1] [k2 v2]]
     {k1 (dmw/deserialize-message v1 (if (vector? proto-class) (first proto-class) proto-class) topic-entity-name)
      k2 (dmw/deserialize-message v2 (if (vector? proto-class) (second proto-class) proto-class) topic-entity-name)})
   message))

(defn protobuf->hash
  "This is a middleware function that takes in a message (Proto ByteArray or PersistentHashMap) and calls the handler-fn with the deserialized PersistentHashMap"
  [handler-fn proto-class topic-entity-name]
  (fn [message]
    (handler-fn (deserialize-stream-joins-message message proto-class topic-entity-name))))
