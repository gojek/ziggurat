(ns ziggurat.clj-statsd-metrics-wrapper
  (:require [clj-statsd :as statsd]
            [ziggurat.metrics-interface])
  (:import (ziggurat.metrics_interface MetricsProtocol)))

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
        final-metric (str namespace "." metric)]
    (statsd/increment final-metric value rate final-tags)))

(defn update-timing [namespace metric tags value]
  (let [final-tags (build-tags tags)
        final-metric (str namespace "." metric)]
    (statsd/timing final-metric value rate final-tags)))

(deftype CljStatsd []
  MetricsProtocol
  (initialize [this statsd-config] (initialize statsd-config))
  (terminate [this] (terminate))
  (update-counter [this namespace metric tags signed-val] (update-counter namespace metric tags signed-val))
  (update-timing [this namespace metric tags value] (update-timing namespace metric tags value)))