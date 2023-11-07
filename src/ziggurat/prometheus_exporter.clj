(ns ziggurat.prometheus-exporter
  (:require [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]
            [iapetos.registry :as reg]
            [clojure.tools.logging :as log]
            [ziggurat.config :refer [prometheus-config]]))

(def registry
  (atom (prometheus/collector-registry)))

(defn get-metric-value [kw-metric]
  (-> @registry kw-metric (prometheus/value)))

(defn register-collecter-if-not-exist [collector label]
  (let [counter (@registry label)]
    (when-not counter
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