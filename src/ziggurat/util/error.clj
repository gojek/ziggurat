(ns ziggurat.util.error
  (:require [sentry-clj.async :as sentry]
            [ziggurat.new-relic :as nr]
            [ziggurat.sentry :refer [sentry-reporter]]))

(defn report-error [throwable message]
  (sentry/report-error sentry-reporter throwable message)
  (nr/report-error throwable message))
