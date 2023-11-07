(ns ziggurat.prometheus-exporter
  (:require [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]
            [iapetos.registry :as reg]))

(def registry
  (atom (prometheus/collector-registry)))

(defn get-metric-value [kw-metric]
  (-> @registry kw-metric (prometheus/value)))

(defn register-collecter-if-not-exist [collector label]
  (let [counter (@registry label)]
    (if counter
      nil
      (let [new-counter (collector label)
            new-registry (prometheus/register @registry new-counter)]
        (reset! registry new-registry)
        new-counter))))

(defn update-counter [namespace metric tags value]
  (let [label (assoc tags :name metric :namespace namespace)]
    (register-collecter-if-not-exist prometheus/gauge label)
    (prometheus/inc (@registry label) value)
    label))

(defn report-histogram [namespace metric tags integer-value]
  (let [label (assoc tags :name metric :namespace namespace)]
    (register-collecter-if-not-exist prometheus/histogram label)
    (prometheus/observe (@registry label) integer-value)))

(defonce httpd
  (standalone/metrics-server @registry {:port 8990}))
