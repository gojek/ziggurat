(ns ziggurat.util.mock-metrics-implementation
  (:require [clojure.test :refer :all])
  (:import (ziggurat.metrics_interface MetricsProtocol)))

(defn initialize [statsd-config]
  nil)

(defn update-counter [namespace metric tags signed-val]
  nil)

(defn update-timing  [namespace metric tags val]
  nil)

(defn terminate []
  nil)

(deftype MockImpl []
  MetricsProtocol
  (initialize [this statsd-config] (initialize statsd-config))
  (terminate [this] (terminate))
  (update-counter [this namespace metric tags signed-val] (update-counter namespace metric tags signed-val))
  (update-timing [this namespace metric tags val] (update-timing namespace metric tags val)))