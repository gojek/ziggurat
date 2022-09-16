(ns ziggurat.tracer
  "This namespace creates a [tracer](https://opentracing.io/docs/overview/tracers/)
   that can be used to trace the various stages of the application workflow.

   The following flows are traced:
   1. Normal basic consume
   2. Retry via rabbitmq
   3. Produce to rabbitmq channel
   4. Produce to another kafka topic

   At the time of initialization, an instance of `io.opentracing Tracer`
   is created based on the configuration.

   - If tracer is disabled, then a `NoopTracer` is created,
     which will basically do nothing.

   - If tracer is enabled, then by default a [Jaeger](https://www.jaegertracing.io/)
     tracer will be created based on the environment variables. Please refer
     [Jaeger Configuration](https://github.com/jaegertracing/jaeger-client-java/tree/master/jaeger-core#configuration-via-environment)
     and [Jaeger Architecture](https://www.jaegertracing.io/docs/1.13/architecture/)
     to set the respective env variables.

     Example Jaeger Env Config:
     `
       JAEGER_SERVICE_NAME: \"service-name\"
       JAEGER_AGENT_HOST: \"localhost\"
       JAEGER_AGENT_PORT: 6831
     `

   - Custom tracer can be created by passing the name of a custom tracer
     provider function as `:custom-provider`.The corresponding function
     will be executed to create the tracer.

   In the event of any errors, a NoopTracer will be created

   Example tracer configuration:

   `
     :tracer {:enabled               [true :bool]\n
              :custom-provider       \"\"}
   `

   Usage:
   `
      In `ziggurat.streams/traced-handler-fn`, around the execution of the handler function,
      a span is started, activated and finished. If there are trace-id headers in the kafka message,
      this span will be tied to the same trace. If not, a new trace will be started.

      Any part of the handler function execution can be traced as a child span of this activated parent span.
      Please refer to the [doc](https://github.com/opentracing/opentracing-java#starting-a-new-span)
      to understand how to create child spans.

      The trace ids are propagated to rabbitmq using `io.opentracing.contrib.rabbitmq.TracingConnection`.
      Hence rabbitmq flows are also traced.

      The trace ids are propagated back to kafka using `io.opentracing.contrib.kafka.TracingKafkaProducer`.
      Hence push back to kafka flow is traced.
   `"
  (:require [mount.core :refer [defstate]]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.tools.logging :as log])
  (:import [io.jaegertracing Configuration]
           [io.opentracing.noop NoopTracerFactory]
           [io.opentracing Tracer]))

(defn default-tracer-provider []
  (.getTracer (Configuration/fromEnv)))

(defn create-tracer []
  (try
    (let [tracer-config (:tracer (ziggurat-config))
          custom-provider (:custom-provider tracer-config)]
      (if (or (nil? tracer-config) (false? (:enabled tracer-config)))
        (NoopTracerFactory/create)

        (if (or (nil? custom-provider) (empty? custom-provider))
          (default-tracer-provider)

          (let [custom-tracer (apply (resolve (symbol custom-provider)) [])]
            (if-not (instance? Tracer custom-tracer)
              (throw (RuntimeException. "Tracer provider did not return a valid tracer"))
              custom-tracer)))))
    (catch Exception e
      (log/error "Failed to create tracer with exception " e)
      (log/info "Creating Noop tracer")
      (NoopTracerFactory/create))))

(declare tracer)

(defstate tracer
  :start (create-tracer)
  :stop #())
