(ns ziggurat.mapper
  (:require [clojure.string :as str]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.new-relic :as nr]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.util.error :refer [report-error]])
  (:import (java.time Instant)))

(defn- send-msg-to-channel [channels message-payload return-code]
  (when-not (contains? (set channels) return-code)
    (throw (ex-info "Invalid mapper return code" {:code return-code})))
  (producer/publish-to-channel-instant-queue return-code message-payload))

(defn mapper-func [user-handler-fn channels]
  (fn [{:keys [topic-entity] :as message-payload}]
    (let [service-name                        (:app-name (ziggurat-config))
          topic-entity-name                   (name topic-entity)
          new-relic-transaction-name          (str topic-entity-name ".handler-fn")
          message-processing-namespace        "message-processing"
          base-metric-namespaces              [service-name topic-entity-name]
          message-processing-namespaces       (conj base-metric-namespaces message-processing-namespace)
          additional-tags                     {:topic_name topic-entity-name}
          success-metric                      "success"
          retry-metric                        "retry"
          skip-metric                         "skip"
          failure-metric                      "failure"
          multi-message-processing-namespaces [message-processing-namespaces [message-processing-namespace]]
          user-payload                        (-> message-payload
                                                  (dissoc :headers)
                                                  (dissoc :topic-entity))]
      (nr/with-tracing "job" new-relic-transaction-name
        (try
          (let [start-time                      (.toEpochMilli (Instant/now))
                return-code                     (user-handler-fn user-payload)
                end-time                        (.toEpochMilli (Instant/now))
                time-val                        (- end-time start-time)
                execution-time-namespace        "handler-fn-execution-time"
                multi-execution-time-namespaces [(conj base-metric-namespaces execution-time-namespace)
                                                 [execution-time-namespace]]]
            (metrics/multi-ns-report-histogram multi-execution-time-namespaces time-val additional-tags)
            (case return-code
              :success (metrics/multi-ns-increment-count multi-message-processing-namespaces success-metric additional-tags)
              :retry   (do (metrics/multi-ns-increment-count multi-message-processing-namespaces retry-metric additional-tags)
                           (producer/retry message-payload))
              :skip    (metrics/multi-ns-increment-count multi-message-processing-namespaces skip-metric additional-tags)
              :block   'TODO
              (do
                (send-msg-to-channel channels message-payload return-code)
                (metrics/multi-ns-increment-count multi-message-processing-namespaces success-metric additional-tags))))
          (catch Throwable e
            (producer/retry message-payload)
            (report-error e (str "Actor execution failed for " topic-entity-name))
            (metrics/multi-ns-increment-count multi-message-processing-namespaces failure-metric additional-tags)))))))

(defn channel-mapper-func [user-handler-fn channel]
  (fn [{:keys [topic-entity] :as message-payload}]
    (let [service-name                        (:app-name (ziggurat-config))
          topic-entity-name                   (name topic-entity)
          channel-name                        (name channel)
          message-processing-namespace        "message-processing"
          base-metric-namespaces              [service-name topic-entity-name channel-name]
          message-processing-namespaces       (conj base-metric-namespaces message-processing-namespace)
          additional-tags                     {:topic_name topic-entity-name :channel_name channel-name}
          metric-namespace                    (str/join "." message-processing-namespaces)
          success-metric                      "success"
          retry-metric                        "retry"
          skip-metric                         "skip"
          failure-metric                      "failure"
          multi-message-processing-namespaces [message-processing-namespaces [message-processing-namespace]]
          user-payload                        (-> message-payload
                                                  (dissoc :topic-entity))]
      (nr/with-tracing "job" metric-namespace
        (try
          (let [start-time                     (.toEpochMilli (Instant/now))
                return-code                    (user-handler-fn user-payload)
                end-time                       (.toEpochMilli (Instant/now))
                time-val                       (- end-time start-time)
                execution-time-namespace       "execution-time"
                multi-execution-time-namespace [(conj base-metric-namespaces execution-time-namespace)
                                                [execution-time-namespace]]]
            (metrics/multi-ns-report-histogram multi-execution-time-namespace time-val additional-tags)
            (case return-code
              :success (metrics/multi-ns-increment-count multi-message-processing-namespaces success-metric additional-tags)
              :retry   (do (metrics/multi-ns-increment-count multi-message-processing-namespaces retry-metric additional-tags)
                           (producer/retry-for-channel message-payload channel))
              :skip    (metrics/multi-ns-increment-count multi-message-processing-namespaces skip-metric additional-tags)
              :block   'TODO
              (throw (ex-info "Invalid mapper return code" {:code return-code}))))
          (catch Throwable e
            (producer/retry-for-channel message-payload channel)
            (report-error e (str "Channel execution failed for " topic-entity-name " and for channel " channel-name))
            (metrics/multi-ns-increment-count multi-message-processing-namespaces failure-metric additional-tags)))))))

(defrecord MessagePayload [message topic-entity])
