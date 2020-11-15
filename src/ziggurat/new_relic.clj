(ns ziggurat.new-relic
  (:require [new-reliquary.core :as newrelic]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.core.async :refer [go]]
            [clojure.tools.logging :as log])
  (:import (java.util HashMap)
           (java.util.concurrent ExecutorService)
           [com.newrelic.api.agent NewRelic]))

(defmacro with-tracing [category transaction-name & body]
  `(newrelic/with-newrelic-transaction
     ~category ~transaction-name (^{:once true} fn* [] ~@body)))

(defn- notice-error [^Throwable throwable message]
  (NewRelic/noticeError throwable (HashMap. {"error_message" message}) false))

(defn report-error [throwable message]
  (when (get-in (ziggurat-config) [:new-relic :report-errors])
    (notice-error throwable message)))

