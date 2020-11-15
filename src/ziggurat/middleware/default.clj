(ns ziggurat.middleware.default
  (:require [protobuf.impl.flatland.mapdef :as protodef]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [get-in-config ziggurat-config]]
            [ziggurat.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.util.error :refer [report-error]]))

(defn deserialize-message
  "This function takes in the message(proto Byte Array) and the proto-class and deserializes the proto ByteArray into a
  Clojure PersistentHashMap.
  Temporary logic for migration of services to Ziggurat V3.0
    If the message is of type map, the function just returns the map as it is. In older versions of Ziggurat (< 3.0) we stored
    the messages in deserialized formats in RabbitMQ and those messages can be processed by this function. So we have this logic here."
  [message proto-class topic-entity-name]
  (if-not (map? message) ;; TODO: we should have proper dispatch logic per message type (not like this)
    (try
      (let [proto-klass  (protodef/mapdef proto-class)
            loaded-proto (protodef/parse proto-klass message)
            proto-keys   (-> proto-klass
                             protodef/mapdef->schema
                             :fields
                             keys)]
        (select-keys loaded-proto proto-keys))
      (catch Throwable e
        (let [service-name      (:app-name (ziggurat-config))
              additional-tags   {:topic_name topic-entity-name}
              default-namespace "message-parsing"
              metric-namespaces [service-name topic-entity-name default-namespace]
              multi-namespaces  [metric-namespaces [default-namespace]]]
          (report-error e (str "Couldn't parse the message with proto - " proto-class))
          (metrics/multi-ns-increment-count multi-namespaces "failed" additional-tags)
          nil)))
    message))

(defn protobuf->hash
  "This is a middleware function that takes in a message (Proto ByteArray or PersistentHashMap) and calls the handler-fn with the deserialized PersistentHashMap"
  [handler-fn proto-class topic-entity-name]
  (fn [message]
    (handler-fn (deserialize-message message proto-class topic-entity-name))))
