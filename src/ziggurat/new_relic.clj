(ns ziggurat.new-relic
  (:require [new-reliquary.core :as newrelic]))

(defmacro with-tracing [category transaction-name & body]
  `(newrelic/with-newrelic-transaction
     ~category ~transaction-name (^{:once true} fn* [] ~@body)))
