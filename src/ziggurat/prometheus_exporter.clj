(ns ziggurat.prometheus-exporter
  (:require [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]))

(def registry
  (atom (prometheus/collector-registry)))

(defn register-counter-if-not-exist [kwname]
  (let [counter (@registry kwname)]
    (if counter
      counter
      (let [new-counter (prometheus/gauge kwname)
            new-registry (prometheus/register @registry new-counter)]
        (reset! registry new-registry)))))

(defn update-counter [namespace metric value]
  (let [kwname (keyword (str namespace "/" metric))
        _ (register-counter-if-not-exist kwname)]
    (prometheus/inc (@registry kwname) value)
    kwname))

(defonce httpd
  (standalone/metrics-server @registry {:port 8088}))
