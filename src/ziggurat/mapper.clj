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

(defn- send-msg-to-channel [channels message-payload topic-entity return-code]
  (when-not (contains? (set channels) return-code)
    (throw (ex-info "Invalid mapper return code" {:code return-code})))
  (producer/publish-to-channel-instant-queue topic-entity return-code message-payload))

(defn mapper-func [mapper-fn topic-entity channels]
  (fn [message-payload]
    (let [service-name               (:app-name (ziggurat-config))
          topic-entity-name          (name topic-entity)
          new-relic-transaction-name (str topic-entity-name ".handler-fn")
          default-namespace          "message-processing"
          metric-namespaces          [service-name topic-entity-name default-namespace]
          additional-tags            {:topic_name topic-entity-name}
          default-namespaces         [default-namespace]
          success-metric             "success"
          retry-metric               "retry"
          skip-metric                "skip"
          failure-metric             "failure"
          multi-namespaces           [metric-namespaces default-namespaces]]
      (nr/with-tracing "job" new-relic-transaction-name
        (try
          (let [start-time                      (.toEpochMilli (Instant/now))
                return-code                     (mapper-fn (:message message-payload))
                end-time                        (.toEpochMilli (Instant/now))
                time-val                        (- end-time start-time)
                execution-time-namespace        "handler-fn-execution-time"
                multi-execution-time-namespaces [[service-name topic-entity-name execution-time-namespace]
                                                 [execution-time-namespace]]]
            (metrics/multi-ns-report-time multi-execution-time-namespaces time-val additional-tags)
            (case return-code
              :success (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags)
              :retry   (do (metrics/multi-ns-increment-count multi-namespaces retry-metric additional-tags)
                           (producer/retry message-payload topic-entity))
              :skip    (metrics/multi-ns-increment-count multi-namespaces skip-metric additional-tags)
              :block   'TODO
              (do
                (send-msg-to-channel channels message-payload topic-entity return-code)
                (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags))))
          (catch Throwable e
            (producer/retry message-payload topic-entity)
            (sentry/report-error sentry-reporter e (str "Actor execution failed for " topic-entity-name))
            (metrics/multi-ns-increment-count multi-namespaces failure-metric additional-tags)))))))

(defn channel-mapper-func [mapper-fn topic-entity channel]
  (fn [message-payload]
    (let [service-name       (:app-name (ziggurat-config))
          topic-entity-name  (name topic-entity)
          channel-name       (name channel)
          default-namespace  "message-processing"
          base-namespaces    [service-name topic-entity-name channel-name]
          metric-namespaces  (conj base-namespaces default-namespace)
          additional-tags    {:topic_name topic-entity-name}
          default-namespaces [default-namespace]
          metric-namespace   (str/join "." metric-namespaces)
          success-metric     "success"
          retry-metric       "retry"
          skip-metric        "skip"
          failure-metric     "failure"
          multi-namespaces   [metric-namespaces default-namespaces]]
      (nr/with-tracing "job" metric-namespace
        (try
          (let [start-time                      (.toEpochMilli (Instant/now))
                return-code                     (mapper-fn (:message message-payload))
                end-time                        (.toEpochMilli (Instant/now))
                time-val                        (- end-time start-time)
                execution-time-namespace        "execution-time"
                multi-execution-time-namespaces [(conj base-namespaces execution-time-namespace)
                                                 [execution-time-namespace]]]
            (metrics/multi-ns-report-time multi-execution-time-namespaces time-val additional-tags)
            (case return-code
              :success (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags)
              :retry   (do (metrics/multi-ns-increment-count multi-namespaces retry-metric additional-tags)
                           (producer/retry-for-channel message-payload topic-entity channel))
              :skip    (metrics/multi-ns-increment-count multi-namespaces skip-metric additional-tags)
              :block   'TODO
              (throw (ex-info "Invalid mapper return code" {:code return-code}))))
          (catch Throwable e
            (producer/retry-for-channel message-payload topic-entity channel)
            (sentry/report-error sentry-reporter e (str "Channel execution failed for " topic-entity-name " and for channel " channel-name))
            (metrics/multi-ns-increment-count multi-namespaces failure-metric additional-tags)))))))


(defn construct-message-payload
  [message]
  {:message message})

(s/defschema message-payload
  {:message s/Any
   :retry-count s/Int})
