(ns ziggurat.sentry
  "Sentry reporting functions used by both Ziggurat and the actor."
  (:require [clojure.tools.logging :as log]
            [mount.core :refer [defstate]]
            [sentry-clj.async :as sentry]
            [ziggurat.config :refer [ziggurat-config]])
  (:gen-class
   :name tech.gojek.ziggurat.Sentry
   :methods [^{:static true} [reportError [Throwable String] void]]))

(defn create-sentry-reporter []
  (let [sentry-config (merge (:sentry (ziggurat-config))
                             {:sync?    false
                              :env      (:env (ziggurat-config))
                              :app-name (:app-name (ziggurat-config))})]
    (log/info "Initialising sentry reporter with config " {:config sentry-config})
    (sentry/create-reporter sentry-config)))

(defstate sentry-reporter
  :start (create-sentry-reporter)
  :stop (when (and (-> (ziggurat-config) :sentry :enabled) sentry-reporter)
          (log/info "Shutting down sentry reporter")
          (sentry/shutdown-reporter sentry-reporter)))

(defmacro report-error
  "Logs an exception and reports it to Sentry.
  Create an exception using ex-info if you don't have an exception but
  still want to report to Sentry."
  [^Throwable error & msgs]
  `(sentry/report-error sentry-reporter ~error ~@msgs))

(defn -reportError [^Throwable error msg]
  (report-error error msg))
