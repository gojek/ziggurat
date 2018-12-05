(ns ziggurat.metrics
  (:require [clojure.tools.logging :as log])
  (:import (com.gojek.metrics.datadog DatadogReporter)
           (com.gojek.metrics.datadog.transport UdpTransport$Builder UdpTransport)
           (java.util.concurrent TimeUnit)
           (io.dropwizard.metrics5 MetricRegistry Meter MetricName Histogram)))

(defonce ^:private group (atom nil))

(defonce metrics-registry
  (MetricRegistry.))

(defn mk-meter
  ([category metric]
   (let [metric-name   (MetricRegistry/name ^String @group ^"[Ljava.lang.String;" (into-array String [category metric]))
         tagged-metric (.tagged ^MetricName metric-name ^"[Ljava.lang.String;" (into-array String ["actor" @group]))]
     (.meter ^MetricRegistry metrics-registry ^MetricName tagged-metric))))

(defn mk-histogram
  ([category metric]
   (let [metric-name   (MetricRegistry/name ^String @group ^"[Ljava.lang.String;" (into-array String [category metric]))
         tagged-metric (.tagged ^MetricName metric-name ^"[Ljava.lang.String;" (into-array String ["actor" @group]))]
     (.histogram ^MetricRegistry metrics-registry tagged-metric))))

(defn increment-count
  ([metric-namespace metric]
   (increment-count metric-namespace metric 1))
  ([metric-namespace metric n]
   (let [meter ^Meter (mk-meter metric-namespace metric)]
     (.mark meter (int n)))))

(defn decrement-count
  ([metric-namespace metric]
   (decrement-count metric-namespace metric 1))
  ([metric-namespace metric n]
   (let [meter ^Meter (mk-meter metric-namespace metric)]
     (.mark meter (int (- n))))))

(defn report-time [metric-namespace time-val]
  (let [histogram ^Histogram (mk-histogram metric-namespace "all")]
    (.update histogram (int time-val))))

(defn start-statsd-reporter [statsd-config env app-name]
  (let [{:keys [enabled host port]} statsd-config]
    (when enabled
      (let [transport (-> (UdpTransport$Builder.)
                          (.withStatsdHost host)
                          (.withPort port)
                          (.build))

            reporter  (-> (DatadogReporter/forRegistry metrics-registry)
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
