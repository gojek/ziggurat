(ns ziggurat.middleware.default
  (:require [flatland.protobuf.core :as proto]
            [sentry-clj.async :as sentry]
            [ziggurat.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]))

(defn- deserialise-message
  "This function takes in the message(proto Byte Array) and the proto-class and deserializes the proto ByteArray into a
  Clojure PersistentHashMap.
  Temporary logic for migration of services to Ziggurat V3.0
    If the message is of type map, the function just returns the map as it is. In older versions of Ziggurat (< 3.0) we stored
    the messages in deserialized formats in RabbitMQ and those messages can be processed by this function. So we have this logic here."
  [message proto-class topic-entity-name]
  (if-not (map? message)
    (try
      (let [proto-klass  (proto/protodef proto-class)
            loaded-proto (proto/protobuf-load proto-klass message)
            proto-keys   (-> proto-klass
                             proto/protobuf-schema
                             :fields
                             keys)]
        (select-keys loaded-proto proto-keys))
      (catch Throwable e
        (let [additional-tags   {:topic_name topic-entity-name}
              default-namespace "message-parsing"]
          (sentry/report-error sentry-reporter e (str "Couldn't parse the message with proto - " proto-class))
          (metrics/increment-count default-namespace "failed" additional-tags)
          nil)))
    message))

(defn protobuf->hash
  "This is a middleware function that takes in a message (Proto ByteArray or PersistentHashMap) and calls the handler-fn with the deserialized PersistentHashMap"
  [handler-fn proto-class topic-entity-name]
  (fn [message]
    (handler-fn (deserialise-message message proto-class topic-entity-name))))
