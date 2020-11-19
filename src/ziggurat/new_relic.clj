(ns ziggurat.new-relic
  (:require [new-reliquary.core :as newrelic]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log])
  (:import (java.util HashMap)
           [com.newrelic.api.agent NewRelic]))

(defmacro with-tracing [category transaction-name & body]
  `(newrelic/with-newrelic-transaction
     ~category ~transaction-name (^{:once true} fn* [] ~@body)))

(defn- notice-error [^Throwable throwable message]
  (let [^Runnable task #(try
                          (with-tracing "reporting" "Error Reported"
                            (NewRelic/noticeError throwable (HashMap. {"error_message" message}) false))
                          (catch Exception e
                            (log/warn e "Error while reporting error to new-relic")))]
    (.start (Thread. task))))

(defn report-error [throwable message]
  (when (get-in (ziggurat-config) [:new-relic :report-errors])
    (notice-error throwable message)))
