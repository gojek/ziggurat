(ns ziggurat.tracer
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]])
  (:import (io.jaegertracing Configuration)
           (io.opentracing.noop NoopTracerFactory)))

(defn- create-tracer []
  (let [jaeger-service-name (-> (ziggurat-config) :tracer :jaeger_service_name)]
    (if (nil? jaeger-service-name)
      (NoopTracerFactory/create)
      (.getTracer (Configuration/fromEnv jaeger-service-name)))))

(defstate tracer
  :start (create-tracer)
  :stop #())
