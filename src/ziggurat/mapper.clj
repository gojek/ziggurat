(ns ziggurat.mapper
  (:require [lambda-common.metrics :as metrics]
            [ziggurat.new-relic :as nr]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.sentry :refer [sentry-reporter]]
            [sentry.core :as sentry]
            [clojure.tools.logging :as log]))

(defn mapper-func [mapper-fn]
  (fn [message]
    (nr/with-tracing "job" "Publish metrics"
      (try
        (let [return-code (mapper-fn message)]
          (case return-code
            :success (metrics/message-successfully-processed!)
            :retry (do (metrics/message-unsuccessfully-processed!)
                       (producer/retry message))
            :skip 'TODO
            :block 'TODO
            (throw (ex-info "Invalid mapper return code" {:code return-code}))))
        (catch Throwable e
          (sentry/report-error sentry-reporter e "Actor execution failed")
          (metrics/message-unsuccessfully-processed!))))))
