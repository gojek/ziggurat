(ns ziggurat.prometheus.core
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config]]
            [iapetos.core :as prometheus]
            [iapetos.standalone :as standalone]
            [ziggurat.prometheus.metrics :as metrics]
            [mount.core :as mount :refer [defstate]]))

(def reg (atom nil))

(defn register-metrics
  "register-metrics registers the ziggurat metrics with the provided prometheus registry, else to the default registry.
  It returns the registry with all the metrics registered."
  ([]
   (register-metrics prometheus/default-registry))
  ([registry]
   (swap! reg (fn [_] (reduce
                       (fn [r m] (prometheus/register r m))
                       registry (metrics/all))))
   @reg))

(defn- start [port]
  (log/info "Starting standalone server on port:" port)
  (standalone/metrics-server @reg {:port 8081}))

(defn- stop
  [server]
  (.close server)
  (log/info "Stopped standalone server"))

(defstate server
  :start (start (-> (ziggurat-config) :prometheus))
  :stop (stop server))
