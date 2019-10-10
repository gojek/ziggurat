(ns ziggurat.middleware.json
  (:require [cheshire.core :refer :all]
            [sentry-clj.async :as sentry]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.metrics :as metrics]))

(defn- deserialize-json
  [message topic-entity-name key-fn]
  (try
    (parse-string message key-fn)
    (catch Exception e
      (let [additional-tags   {:topic_name topic-entity-name}
            default-namespace "json-message-parsing"]
        (sentry/report-error sentry-reporter e (str "Could not parse JSON message " message))
        (metrics/increment-count default-namespace "failed" additional-tags)
        nil))))

(defn parse-json
  "This method returns a function which deserializes the provided message before processing it.
   It uses `deserialize-json` defined in this namespace to parse JSON strings."
  [handler-fn topic-entity-name key-fn]
  (fn [message]
    (handler-fn (deserialize-json message topic-entity-name key-fn))))
