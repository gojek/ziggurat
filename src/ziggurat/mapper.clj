(ns ziggurat.mapper
  (:require [clojure.string :as str]
            [schema.core :as s]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.new-relic :as nr]
            [ziggurat.sentry :refer [sentry-reporter]])
  (:import (java.time Instant)))

(defn- send-msg-to-channel [channels message-payload return-code]
  (when-not (contains? (set channels) return-code)
    (throw (ex-info "Invalid mapper return code" {:code return-code})))
  (producer/publish-to-channel-instant-queue return-code message-payload))

(defn mapper-func [mapper-fn channels]
  (fn [{:keys [topic-entity message] :as message-payload}]
    (let [topic-entity-name          (name topic-entity)
          new-relic-transaction-name (str topic-entity-name ".handler-fn")
          default-namespace          "message-processing"
          additional-tags            {:topic_name topic-entity-name}
          success-metric             "success"
          retry-metric               "retry"
          skip-metric                "skip"
          failure-metric             "failure"]
      (nr/with-tracing "job" new-relic-transaction-name
        (try
          (let [start-time               (.toEpochMilli (Instant/now))
                return-code              (mapper-fn message)
                end-time                 (.toEpochMilli (Instant/now))
                time-val                 (- end-time start-time)
                execution-time-namespace "handler-fn-execution-time"]
            (metrics/report-time execution-time-namespace time-val additional-tags)
            (case return-code
              :success (metrics/increment-count default-namespace success-metric 1 additional-tags)
              :retry   (do (metrics/increment-count default-namespace retry-metric 1 additional-tags)
                           (producer/retry message-payload))
              :skip    (metrics/increment-count default-namespace skip-metric 1 additional-tags)
              :block   'TODO
              (do
                (send-msg-to-channel channels message-payload return-code)
                (metrics/increment-count default-namespace success-metric 1 additional-tags))))
          (catch Throwable e
            (producer/retry message-payload)
            (sentry/report-error sentry-reporter e (str "Actor execution failed for " topic-entity-name))
            (metrics/increment-count default-namespace failure-metric 1 additional-tags)))))))

(defn channel-mapper-func [mapper-fn channel]
  (fn [{:keys [topic-entity message] :as message-payload}]
    (let [service-name      (:app-name (ziggurat-config))
          topic-entity-name (name topic-entity)
          channel-name      (name channel)
          default-namespace "message-processing"
          base-namespaces   [service-name topic-entity-name channel-name]
          metric-namespaces (conj base-namespaces default-namespace)
          additional-tags   {:topic_name topic-entity-name :channel_name channel-name}
          metric-namespace  (str/join "." metric-namespaces)
          success-metric    "success"
          retry-metric      "retry"
          skip-metric       "skip"
          failure-metric    "failure"]
      (nr/with-tracing "job" metric-namespace
        (try
          (let [start-time               (.toEpochMilli (Instant/now))
                return-code              (mapper-fn message)
                end-time                 (.toEpochMilli (Instant/now))
                time-val                 (- end-time start-time)
                execution-time-namespace "execution-time"]
            (metrics/report-time execution-time-namespace time-val additional-tags)
            (case return-code
              :success (metrics/increment-count default-namespace success-metric 1 additional-tags)
              :retry   (do (metrics/increment-count default-namespace retry-metric 1 additional-tags)
                           (producer/retry-for-channel message-payload channel))
              :skip    (metrics/increment-count default-namespace skip-metric 1 additional-tags)
              :block   'TODO
              (throw (ex-info "Invalid mapper return code" {:code return-code}))))
          (catch Throwable e
            (producer/retry-for-channel message-payload channel)
            (sentry/report-error sentry-reporter e (str "Channel execution failed for " topic-entity-name " and for channel " channel-name))
            (metrics/increment-count default-namespace failure-metric 1 additional-tags)))))))

(defrecord MessagePayload [message topic-entity])

(s/defschema message-payload-schema
  {:message                      s/Any
   :topic-entity                 s/Keyword
   (s/optional-key :retry-count) s/Int})
