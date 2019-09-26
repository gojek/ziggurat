(ns ziggurat.tracer
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log])
  (:import [io.jaegertracing Configuration]
           [io.opentracing.noop NoopTracerFactory]
           [io.opentracing Tracer]))

(defn- default-tracer-provider [default-tracer-config]
  (.getTracer (Configuration/fromEnv (:jaeger-service-name default-tracer-config))))

(defn create-tracer []
  (try
    (let [tracer-config (:tracer (ziggurat-config))
          tracer-provider (:tracer-provider tracer-config)]
      (if (or (nil? tracer-config) (false? (:enabled tracer-config)))
        (NoopTracerFactory/create)

        (if (or (nil? tracer-provider) (empty? tracer-provider))
          (default-tracer-provider (:default-tracer-config tracer-config))

          (let [custom-tracer (apply (resolve (symbol tracer-provider)) [])]
            (if-not (instance? Tracer custom-tracer)
              (throw (RuntimeException. "Tracer provider did not return a valid tracer"))
              custom-tracer)))))
    (catch Exception e
      (log/error "Failed to create tracer with exception " e)
      (log/info "Creating Noop tracer")
      (NoopTracerFactory/create))))

(defstate tracer
  :start (create-tracer)
  :stop #())
