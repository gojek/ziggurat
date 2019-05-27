(ns ziggurat.mapper
  (:require [sentry-clj.async :as sentry]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.new-relic :as nr]
            [ziggurat.sentry :refer [sentry-reporter]])
  (:import (java.time Instant)))

(defn- send-msg-to-channel [channels message topic-entity return-code]
  (when-not (contains? (set channels) return-code)
    (throw (ex-info "Invalid mapper return code" {:code return-code})))
  (producer/publish-to-channel-instant-queue topic-entity return-code message))

(defn mapper-func [mapper-fn topic-entity channels]
  (fn [message]
    (let [topic-entity-name          (name topic-entity)
          new-relic-transaction-name (str topic-entity-name ".handler-fn")
          default-namespace          "message-processing"
          metric-namespaces          [topic-entity-name default-namespace]
          additional-tags            {:topic-name topic-entity-name}
          default-namespaces         [default-namespace]
          success-metric             "success"
          retry-metric               "retry"
          skip-metric                "skip"
          failure-metric             "failure"
          multi-namespaces           [metric-namespaces default-namespaces]]
      (nr/with-tracing "job" new-relic-transaction-name
        (try
          (let [start-time                      (.toEpochMilli (Instant/now))
                return-code                     (mapper-fn message)
                end-time                        (.toEpochMilli (Instant/now))
                time-val                        (- end-time start-time)
                execution-time-namespace        "handler-fn-execution-time"
                multi-execution-time-namespaces [[topic-entity-name execution-time-namespace]
                                                 [execution-time-namespace]]]
            (metrics/multi-ns-report-time multi-execution-time-namespaces time-val)
            (case return-code
              :success (do (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags))
              :retry   (do (metrics/multi-ns-increment-count multi-namespaces retry-metric additional-tags)
                           (producer/retry message topic-entity))
              :skip    (do (metrics/multi-ns-increment-count multi-namespaces skip-metric additional-tags))
              :block   'TODO
              (do
                (send-msg-to-channel channels message topic-entity return-code)
                (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags))))
          (catch Throwable e
            (producer/retry message topic-entity)
            (sentry/report-error sentry-reporter e (str "Actor execution failed for " topic-entity-name))
            (metrics/multi-ns-increment-count multi-namespaces failure-metric additional-tags)))))))

(defn channel-mapper-func [mapper-fn topic-entity channel]
  (fn [message]
    (let [topic-entity-name  (name topic-entity)
          channel-name       (name channel)
          default-namespace  "message-processing"
          base-namespaces    [topic-entity-name channel-name]
          metric-namespaces  (conj base-namespaces default-namespace)
          additional-tags    {:topic-name topic-entity-name}
          default-namespaces [default-namespace]
          metric-namespace   (apply str (interpose "." metric-namespaces))
          success-metric     "success"
          retry-metric       "retry"
          skip-metric        "skip"
          failure-metric     "failure"
          multi-namespaces   [metric-namespaces default-namespaces]]
      (nr/with-tracing "job" metric-namespace
        (try
          (let [start-time                      (.toEpochMilli (Instant/now))
                return-code                     (mapper-fn message)
                end-time                        (.toEpochMilli (Instant/now))
                time-val                        (- end-time start-time)
                execution-time-namespace        "execution-time"
                multi-execution-time-namespaces [(conj base-namespaces execution-time-namespace)
                                                 [execution-time-namespace]]]
            (metrics/multi-ns-report-time multi-execution-time-namespaces time-val)
            (case return-code
              :success (do (metrics/multi-ns-increment-count multi-namespaces success-metric additional-tags))
              :retry   (do (metrics/multi-ns-increment-count multi-namespaces retry-metric additional-tags)
                           (producer/retry-for-channel message topic-entity channel))
              :skip    (do (metrics/multi-ns-increment-count multi-namespaces skip-metric additional-tags))
              :block   'TODO
              (throw (ex-info "Invalid mapper return code" {:code return-code}))))
          (catch Throwable e
            (producer/retry-for-channel message topic-entity channel)
            (sentry/report-error sentry-reporter e (str "Channel execution failed for " topic-entity-name " and for channel " channel-name))
            (metrics/multi-ns-increment-count multi-namespaces failure-metric additional-tags)))))))
