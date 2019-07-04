(ns ziggurat.middleware.default
  (:require [flatland.protobuf.core :as proto]
            [sentry-clj.async :as sentry]
            [ziggurat.metrics :as metrics]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.config :refer [ziggurat-config]]))


(defn- deserialise-message [message proto-class]
  (if-not (map? message)
    (let [proto-klass  (proto/protodef proto-class)
          loaded-proto (proto/protobuf-load proto-klass message)
          proto-keys   (-> proto-klass
                           proto/protobuf-schema
                           :fields
                           keys)]
      (select-keys loaded-proto proto-keys))
    message))

(defn protobuf->hash [handler-fn proto-class topic-entity-name]
  (fn [message]
    (try
      (handler-fn (deserialise-message message proto-class))
      (catch Throwable e
        (let [service-name      (:app-name (ziggurat-config))
              additional-tags   {:topic_name topic-entity-name}
              default-namespace "message-parsing"
              metric-namespaces [service-name "message-parsing"]
              multi-namespaces  [metric-namespaces [default-namespace]]]
          (sentry/report-error sentry-reporter e (str "Couldn't parse the message with proto - " proto-class))
          (metrics/multi-ns-increment-count multi-namespaces "failed" additional-tags)
          nil)))))