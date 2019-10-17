(ns ziggurat.middleware.json
  "This namespace defines middleware methods for parsing JSON strings.
   Please see [Ziggurat Middleware](https://github.com/gojek/ziggurat#middleware-in-ziggurat) for more details.
  "
  (:require [cheshire.core :refer :all]
            [sentry-clj.async :as sentry]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.metrics :as metrics]))

(defn- deserialize-json
  [message topic-entity key-fn]
  (try
    (parse-string message key-fn)
    (catch Exception e
      (let [additional-tags   {:topic_name topic-entity}
            default-namespace "json-message-parsing"]
        (sentry/report-error sentry-reporter e (str "Could not parse JSON message " message))
        (metrics/increment-count default-namespace "failed" additional-tags)
        nil))))

(defn parse-json
  "This method returns a function which deserializes the provided message before processing it.
   It uses `deserialize-json` defined in this namespace to parse JSON strings.

   Takes following arguments:
   `handler-fn`    : A function which would process the parsed JSON string
   `topic-entity`  : Stream route topic entity (as defined in config.edn). It is only used for publishing metrics.
   `key-fn`        : key-fn can be either true, false of a generic function to transform keys in JSON string.
                         If `true`, would coerce keys to keywords, and if `false` it would leave keys as strings.
                         Default value is true.

   An Example
   For parsing a JSON string `\"{\"foo\":\"bar\"}\"` to `{\"foo\":\"bar\"}`, call the function with `key-fn` false
   `(parse-json (fn [message] ()) \"topic\" false)`

   For parsing a JSON string `\"{\"foo\":\"bar\"}\"` to `{:foo:\"bar\"}`, call the function without passing `key-fn`,
   whose default value is true.
   `(parse-json (fn [message] ()) \"topic\")`

   "
  ([handler-fn topic-entity]
   (parse-json handler-fn topic-entity true))
  ([handler-fn topic-entity key-fn]
   (fn [message]
     (handler-fn (deserialize-json message topic-entity key-fn)))))
