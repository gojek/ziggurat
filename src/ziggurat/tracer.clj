(ns ziggurat.tracer
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]])
  (:import (io.jaegertracing Configuration)
           (io.opentracing.noop NoopTracerFactory)))

(defn- create-tracer []
  (let [tracer-config (:tracer (ziggurat-config))]
    (if (or (nil? tracer-config) (false? (:enabled tracer-config)))
      (NoopTracerFactory/create)
      (.getTracer (Configuration/fromEnv (:jaeger_service_name tracer-config))))))

(defstate tracer
  :start (create-tracer)
  :stop #())
