(ns ziggurat.metrics
  (:require [clojure.tools.logging :as log]
            [clojure.walk :refer [stringify-keys]])
  (:import (com.gojek.metrics.datadog DatadogReporter)
           (com.gojek.metrics.datadog.transport UdpTransport$Builder UdpTransport)
           (io.dropwizard.metrics5 MetricRegistry Meter MetricName Histogram)
           (java.util.concurrent TimeUnit)))

(defonce ^:private group (atom nil))

(defonce metrics-registry
  (MetricRegistry.))

(defn- merge-tags
  [additional-tags]
  (let [default-tags {"actor" @group}]
    (merge default-tags (when (not (empty? additional-tags))
                          (stringify-keys additional-tags)))))

(defn mk-meter
  ([category metric]
   (mk-meter category metric nil))
  ([category metric additional-tags]
   (let [metric-name   (MetricRegistry/name ^String @group ^"[Ljava.lang.String;" (into-array String [category metric]))
         tags          (merge-tags additional-tags)
         tagged-metric (.tagged ^MetricName metric-name tags)]
     (.meter ^MetricRegistry metrics-registry ^MetricName tagged-metric))))

(defn mk-histogram
  ([category metric]
   (mk-histogram category metric nil))
  ([category metric additional-tags]
   (let [metric-name   (MetricRegistry/name ^String @group ^"[Ljava.lang.String;" (into-array String [category metric]))
         tags          (merge-tags additional-tags)
         tagged-metric (.tagged ^MetricName metric-name tags)]
     (.histogram ^MetricRegistry metrics-registry tagged-metric))))

(defn- intercalate-dot
  [names]
  (apply str (interpose "." names)))

(defn- inc-or-dec-count
  [sign metric-namespaces metric additional-tags]
  (let [metric-namespace (intercalate-dot metric-namespaces)
        meter            ^Meter (mk-meter metric-namespace metric additional-tags)]
    (.mark meter (sign 1))))

(def increment-count (partial inc-or-dec-count +))

(def decrement-count (partial inc-or-dec-count -))

(defn multi-ns-increment-count [nss metric additional-tags]
  (doseq [ns nss]
    (increment-count ns metric additional-tags)))

(defn report-time
  [metric-namespaces time-val additional-tags]
  (let [metric-namespace (intercalate-dot metric-namespaces)
        histogram        ^Histogram (mk-histogram metric-namespace "all" additional-tags)]
    (.update histogram (int time-val))))

(defn multi-ns-report-time
  ([nss time-val]
   (multi-ns-report-time nss time-val nil))
  ([nss time-val additional-tags]
   (doseq [ns nss]
     (report-time ns time-val additional-tags))))

(defn start-statsd-reporter [statsd-config env app-name]
  (let [{:keys [enabled host port]} statsd-config]
    (when enabled
      (let [transport (-> (UdpTransport$Builder.)
                          (.withStatsdHost host)
                          (.withPort port)
                          (.build))

            reporter (-> (DatadogReporter/forRegistry metrics-registry)
                         (.withTransport transport)
                         (.withTags [(str env)])
                         (.build))]
        (log/info "Starting statsd reporter")
        (.start reporter 1 TimeUnit/SECONDS)
        (reset! group app-name)
        {:reporter reporter :transport transport}))))

(defn stop-statsd-reporter [datadog-reporter]
  (when-let [{:keys [reporter transport]} datadog-reporter]
    (.stop ^DatadogReporter reporter)
    (.close ^UdpTransport transport)
    (log/info "Stopped statsd reporter")))
