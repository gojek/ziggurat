(ns ziggurat.tracer
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log])
  (:import [io.jaegertracing Configuration Configuration$ReporterConfiguration Configuration$SenderConfiguration]
           [io.opentracing.noop NoopTracerFactory]
           [io.opentracing Tracer]))

(defn- default-tracer-provider [{:keys [jaeger-service-name jaeger-agent-host jaeger-agent-port jaeger-reporter-log-spans]}]
  (let [sender-configuration (-> (Configuration$SenderConfiguration.)
                                 (.withAgentHost jaeger-agent-host)
                                 (.withAgentPort (int jaeger-agent-port)))
        reporter-configuration (-> (Configuration$ReporterConfiguration.)
                                   (.withLogSpans jaeger-reporter-log-spans)
                                   (.withSender sender-configuration))
        configuration (.withTraceId128Bit (Configuration. jaeger-service-name) true)]
    (.getTracer (.withReporter configuration reporter-configuration))))

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
