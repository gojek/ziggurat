(ns ziggurat.dropwizard-metrics-wrapper
  (:require [clojure.tools.logging :as log]
            [ziggurat.metrics-interface])
  (:import [com.codahale.metrics Histogram Meter MetricRegistry]
           [java.util.concurrent TimeUnit]
           [org.coursera.metrics.datadog DatadogReporter]
           [org.coursera.metrics.datadog.transport UdpTransport UdpTransport$Builder]
           (ziggurat.metrics_interface MetricsProtocol)))

(defonce metrics-registry
  (MetricRegistry.))

(def reporter-and-transport-state (atom nil))

(defn initialize [statsd-config]
  (let [{:keys [enabled host port]} statsd-config]
    (when enabled
      (let [transport (-> (UdpTransport$Builder.)
                          (.withStatsdHost host)
                          (.withPort port)
                          (.build))

            reporter  (-> (DatadogReporter/forRegistry metrics-registry)
                          (.withTransport transport)
                          (.build))]
        (log/info "Starting statsd reporter")
        (.start reporter 1 TimeUnit/SECONDS)
        (reset! reporter-and-transport-state {:reporter reporter :transport transport})))))

(defn terminate []
  (when-let [{:keys [reporter transport]} @reporter-and-transport-state]
    (.stop ^DatadogReporter reporter)
    (.close ^UdpTransport transport)
    (reset! reporter-and-transport-state nil)
    (log/info "Stopped statsd reporter")))

(defn mk-meter
  ([category metric]
   (mk-meter category metric {}))
  ([category metric _]
   (let [namespace        (str category "." metric)
         metric-name      (MetricRegistry/name ^String namespace nil)]
     (.meter ^MetricRegistry metrics-registry metric-name))))

(defn mk-histogram
  ([category metric]
   (mk-histogram category metric {}))
  ([category metric _]
   (let [namespace        (str category "." metric)
         metric-name      (MetricRegistry/name ^String namespace nil)]
     (.histogram ^MetricRegistry metrics-registry metric-name))))

(defn update-counter
  [namespace metric tags value]
  (let [meter (mk-meter namespace metric tags)]
    (.mark ^Meter meter value)))

(defn update-histogram
  [namespace metric tags value]
  (let [histogram (mk-histogram namespace metric tags)]
    (.update ^Histogram histogram value)))

(deftype DropwizardMetrics []
  MetricsProtocol
  (initialize [_ statsd-config] (initialize statsd-config))
  (terminate [_] (terminate))
  (update-counter [_ namespace metric tags value] (update-counter namespace metric tags value))
  (update-timing [_ namespace metric tags value] (update-histogram namespace metric tags value)))
