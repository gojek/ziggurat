(ns ziggurat.dropwizard-metrics-wrapper
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.walk :refer [stringify-keys]])
  (:import [com.gojek.metrics.datadog.transport UdpTransport UdpTransport$Builder]
           [io.dropwizard.metrics5 MetricRegistry]
           [com.gojek.metrics.datadog DatadogReporter]
           [java.util.concurrent TimeUnit]
           [io.dropwizard.metrics5 Histogram Meter MetricName MetricRegistry]))


(defonce metrics-registry
         (MetricRegistry.))

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
        {:reporter reporter :transport transport}))))

(defn terminate [datadog-reporter]
  (when-let [{:keys [reporter transport]} datadog-reporter]
    (.stop ^DatadogReporter reporter)
    (.close ^UdpTransport transport)
    (log/info "Stopped statsd reporter")))


;;TODO: remove this from here, we need to pass all the tags directly to the library instead of us constructing them here
(defn- merge-tags
  [additional-tags]
  (let [{:keys [app-name env]} (ziggurat-config)
        default-tags {"actor" app-name
                      "env"   env}]
    (merge default-tags (stringify-keys additional-tags))))

(defn- get-tagged-metric
  [metric-name tags]
  (.tagged ^MetricName metric-name tags))

(defn mk-meter
  ([category metric]
   (mk-meter category metric nil))
  ([category metric additional-tags]
   (let [namespace     (str category "." metric)
         metric-name   (MetricRegistry/name ^String namespace nil)
         tags          (merge-tags additional-tags)
         tagged-metric (get-tagged-metric metric-name tags)]
     (.meter ^MetricRegistry metrics-registry ^MetricName tagged-metric))))

(defn mk-histogram
  ([category metric]
   (mk-histogram category metric nil))
  ([category metric additional-tags]
   (let [namespace     (str category "." metric)
         metric-name   (MetricRegistry/name ^String namespace nil)
         tags          (merge-tags additional-tags)
         tagged-metric (.tagged ^MetricName metric-name tags)]
     (.histogram ^MetricRegistry metrics-registry ^MetricName tagged-metric))))

