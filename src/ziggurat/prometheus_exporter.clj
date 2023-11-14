(ns ziggurat.prometheus-exporter
  (:require [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]
            [iapetos.registry :as reg]
            [clojure.tools.logging :as log]
            [ziggurat.config :refer [prometheus-config]]))

(def registry
  (atom (prometheus/collector-registry)))

(defn get-metric-value [kw-metric labels]
  (prometheus/value (@registry kw-metric labels)))

(defn register-collecter-if-not-exist [collector metric-name label-vec]
  (let [counter (@registry metric-name {:labels label-vec})]
    (when-not counter
      (let [new-counter (collector metric-name {:labels label-vec})
            new-registry (prometheus/register @registry new-counter)]
        (reset! registry new-registry)
        new-counter))))

(defn update-counter [namespace metric tags value]
  (let [metric-name {:name metric :namespace namespace}
        label-vec (vec (keys tags))]
    (register-collecter-if-not-exist prometheus/gauge metric-name label-vec)
    (prometheus/inc (@registry metric-name tags) value)
    metric-name))

(defn report-histogram [namespace metric tags integer-value]
  (let [metric-name {:name metric :namespace namespace}
        label-vec (vec (keys tags))]
    (register-collecter-if-not-exist prometheus/histogram metric-name label-vec)
    (prometheus/observe (@registry metric-name tags) integer-value)))

(defonce httpd (atom nil))

(defn start-http-server []
  (when-not @httpd
    (reset! httpd (standalone/metrics-server @registry
                                             {:port
                                              (get (prometheus-config)
                                                   :port 8002)}))))

(defn stop-http-server []
  (when-let [server @httpd]
    (.close server)
    (reset! httpd nil)))