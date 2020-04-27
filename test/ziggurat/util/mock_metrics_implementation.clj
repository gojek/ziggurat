(ns ziggurat.util.mock-metrics-implementation
  (:require [clojure.test :refer :all])
  (:import (ziggurat.metrics_interface MetricsLib)))

(defn initialize [statsd-config]
  nil)

(defn update-counter [namespace metric tags signed-val]
  nil)

(defn update-timing  [namespace metric tags signed-val]
  nil)

(defn terminate []
  nil)

(deftype MockImpl []
  MetricsLib
  (initialize [this statsd-config] (initialize statsd-config))
  (terminate [this] (terminate))
  (update-counter [this namespace metric tags signed-val] (update-counter namespace metric tags signed-val))
  (update-histogram [this namespace metric tags signed-val] (update-timing namespace metric tags signed-val)))