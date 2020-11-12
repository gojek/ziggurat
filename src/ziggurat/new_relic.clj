(ns ziggurat.new-relic
  (:require [new-reliquary.core :as newrelic]
            [ziggurat.config :refer [ziggurat-config]])
  (:import (java.util HashMap)
           [com.newrelic.api.agent NewRelic]))

(defmacro with-tracing [category transaction-name & body]
  `(newrelic/with-newrelic-transaction
     ~category ~transaction-name (^{:once true} fn* [] ~@body)))

(defn- notice-error [throwable message]
  (NewRelic/noticeError throwable (HashMap. {"error_message" message}) false))

(defn report-error [throwable message]
  (when (get-in (ziggurat-config) [:new-relic :report-errors])
    (notice-error throwable message)))

