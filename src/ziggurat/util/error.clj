(ns ziggurat.util.error
  (:require [sentry-clj.async :as sentry]
            [ziggurat.new-relic :as nr]
            [ziggurat.sentry :refer [sentry-reporter]]
            [clojure.tools.logging :as log]))

(defn report-error [throwable message]
  (log/error throwable message)
  (nr/report-error throwable message))
