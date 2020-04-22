(ns ziggurat.clj-statsd-metrics-wrapper
  (:require [clj-statsd :as statsd])
  (:import (ziggurat.metrics_interface MetricsLib)))

(def rate 1.0)

(defn initialize [{:keys [host port enabled]}]
  (when enabled
    (statsd/setup host port)))

(defn terminate []
  (reset! statsd/cfg nil)
  (send statsd/sockagt (constantly nil)))

(defn- coerce-value
  [v]
  (if (or (keyword? v)
          (symbol? v))
    (name v)
    v))

(defn- build-tags
  [tags]
  (map (fn [[key value]]
         (str (name key) ":" (coerce-value value)))
       tags))

(defn update-counter [namespace metric tags value]
  (let [final-tags (build-tags tags)
        signed-value value
        final-metric (str namespace "." metric)]
    (statsd/increment final-metric signed-value rate final-tags)))

(deftype CljStatsd []
  MetricsLib
  (initialize [this statsd-config] (initialize statsd-config))
  (terminate [this] (terminate))
  (update-counter [this namespace metric tags signed-val] (update-counter namespace metric tags signed-val)))