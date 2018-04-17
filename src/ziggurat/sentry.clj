(ns ziggurat.sentry
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [config]]
            [mount.core :refer [defstate]]
            [sentry.core :as sentry])
  (:import (com.google.common.util.concurrent ThreadFactoryBuilder)
           (java.util.concurrent ThreadFactory)))

(defn create-sentry-reporter []
  (let [sentry-config (merge (:sentry config)
                             {:sync?    false
                              :env      (:env config)
                              :app-name (:app-name config)})]
    (log/info "Initialising sentry reporter with config " {:config sentry-config})
    (sentry/create-reporter sentry-config)))

(defstate sentry-reporter
  :start (create-sentry-reporter)
  :stop (when (and (-> config :sentry :enabled) sentry-reporter)
          (log/info "Shutting down sentry reporter")
          (sentry/shutdown-reporter sentry-reporter)))
