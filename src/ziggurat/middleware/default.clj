(ns ziggurat.middleware.default
  (:require [protobuf.impl.flatland.mapdef :as protodef]
            [sentry-clj.async :as sentry]
            [ziggurat.kafka-delay :refer [calculate-and-report-kafka-delay]]
            [ziggurat.config :refer [get-in-config ziggurat-config]]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer [get-current-time-in-millis get-timestamp-from-record]]
            [ziggurat.streams :refer [default-config-for-stream]]
            [ziggurat.sentry :refer [sentry-reporter]]))

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
          (sentry/report-error sentry-reporter e (str "Couldn't parse the message with proto - " proto-class))
          (metrics/multi-ns-increment-count multi-namespaces "failed" additional-tags)
          nil)))
    message))

(defn- message-to-process? [message-timestamp oldest-processed-message-in-s]
  (let [current-time (get-current-time-in-millis)
        allowed-time (- current-time (* 1000 oldest-processed-message-in-s))]
    (> message-timestamp allowed-time)))


(defn protobuf->hash
  "This is a middleware function that takes in a message (Proto ByteArray or PersistentHashMap) and calls the handler-fn with the deserialized PersistentHashMap"
  [handler-fn proto-class topic-entity-name]
  (fn [message]
    (let [metadata (meta message)]
      (when (message-to-process? (:timestamp metadata) (:oldest-processed-message-in-s default-config-for-stream) )
        (calculate-and-report-kafka-delay (:metric-namespace metadata) (:timestamp metadata) (:additional-tags metadata))
        (handler-fn (deserialize-message message proto-class topic-entity-name))))))
