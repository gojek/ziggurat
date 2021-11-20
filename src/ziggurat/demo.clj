(ns ziggurat.demo
  (:require [iapetos.core :as prometheus]
            [ziggurat.init :refer [main]]
            [ziggurat.prometheus.core :as p]
            [ziggurat.middleware.default :as mw])
  (:import [com.gojek.esb.driverprofile DriverProfileLogMessage])
  (:gen-class
   :methods [^{:static true} [init [java.util.Map] void]]
   :name tech.gojek.ziggurat.internal.Init))




(defn my-start []
  (swap! p/reg (fn [r] (prometheus/register r (prometheus/counter :ziggurat/my-metric {:description "my-metric"
                                                                                       :labels      [:topic-name :actor :env]})))))
(defn my-stop [])

(defn my-print-foo
  [message]
  (prometheus/inc @p/reg :ziggurat/my-metric {:topic-entity "foo"})
  (Thread/sleep 150)
  :retry)

(def handler-fn
  (-> my-print-foo (mw/protobuf->hash DriverProfileLogMessage :driver-profile-events)))

(defn -main [& args]
  (p/register-metrics)
  (main my-start my-stop {:driver-profile-events {:handler-fn handler-fn}}))
