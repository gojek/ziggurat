(ns ziggurat.mapper
  (:require [lambda-common.metrics :as metrics]
            [sentry.core :as sentry]
            [ziggurat.new-relic :as nr]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.sentry :refer [sentry-reporter]])
  (:import (java.time Instant)))

(defn mapper-func [mapper-fn]
  (fn [message topic-entity]
    (nr/with-tracing "job" (str topic-entity ".handler-fn")
      (try
        (let [start-time  (.toEpochMilli (Instant/now))
              return-code (mapper-fn message)
              end-time    (.toEpochMilli (Instant/now))]
          (metrics/report-time (str topic-entity ".handler-fn-execution-time") (- end-time start-time))
          (case return-code
            :success (metrics/message-successfully-processed!)
            :retry (do (metrics/message-unsuccessfully-processed!)
                       (producer/retry message topic-entity))
            :skip 'TODO
            :block 'TODO
            (throw (ex-info "Invalid mapper return code" {:code return-code}))))
        (catch Throwable e
          (sentry/report-error sentry-reporter e (str "Actor execution failed for " topic-entity))
          (metrics/message-unsuccessfully-processed!))))))
