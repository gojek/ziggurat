(ns ziggurat.middleware.metrics.stream-joins-diff
  (:require [ziggurat.metrics :as metrics]
            [ziggurat.config :refer [ziggurat-config]]))

(defn- publish-diff-between-joined-messages-helper
  [message]
  (let [keys (keys message)
        values (vals message)
        left-topic-key (first keys)
        left (first values)
        right-topic-key (second keys)
        right (second values)
        service-name       (:app-name (ziggurat-config))
        leftEventTimeStamp (:nanos (:event-timestamp left))
        rightEventTimeStamp (:nanos (:event-timestamp right))
        diff                (Math/abs ^Integer (- leftEventTimeStamp rightEventTimeStamp))]
    (metrics/report-histogram [service-name "stream-joins-message-diff"] diff {:left (name left-topic-key) :right (name right-topic-key)})))

(defn publish-diff-between-joined-messages
  [handler-fn]
  (fn [message]
    (do (publish-diff-between-joined-messages-helper message)
        (handler-fn message))))

