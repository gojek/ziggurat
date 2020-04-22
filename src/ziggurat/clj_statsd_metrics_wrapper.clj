(ns ziggurat.clj-statsd-metrics-wrapper
  (:require [clj-statsd :as statsd])
  (:import (ziggurat.metrics_interface MetricsLib)))


(defn initialize [{:keys [host port enabled]}]
  (when enabled
    (statsd/setup host port)))

(defn terminate []
  (reset! statsd/cfg nil)
  (send statsd/sockagt (constantly nil)))

(deftype CljStatsd []
  MetricsLib
  (initialize [this statsd-config] (initialize statsd-config))
  (terminate [this] (terminate)))
