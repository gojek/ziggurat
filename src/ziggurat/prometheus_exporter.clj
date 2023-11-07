(ns ziggurat.prometheus-exporter
  (:require [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]
            [iapetos.registry :as reg]))

(def registry
  (atom (prometheus/collector-registry)))

(defn get-metric-value [metric-name]
  (.getSampleValue (reg/raw @registry) metric-name))

(defn register-counter-if-not-exist [label]
  (let [counter (@registry label)]
    (if counter
      nil
      (let [new-counter (prometheus/gauge label)
            new-registry (prometheus/register @registry new-counter)]
        (reset! registry new-registry)
        new-counter))))

(defn update-counter [namespace metric value tags]
  (let [label (assoc tags :name metric :namespace namespace)]
    (register-counter-if-not-exist label)
    (prometheus/inc (@registry label) value)
    label))

(defonce httpd
  (standalone/metrics-server @registry {:port 8990}))
