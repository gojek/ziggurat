(ns ziggurat.dropwizard-metrics-wrapper-test
  (:require [ziggurat.dropwizard-metrics-wrapper :as dw-metrics]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.test :refer [deftest is testing]])
  (:import [com.codahale.metrics Meter Histogram]
           (org.coursera.metrics.datadog DatadogReporter)
           (org.coursera.metrics.datadog.transport UdpTransport)))

(deftest mk-meter-test
  (let [category     "category"
        metric       "metric1"
        service-name (:app-name (ziggurat-config))
        tags         {:actor service-name
                      :foo   "bar"}]
    (testing "returns a meter with no tags when none are passed to it"
      (is (instance? Meter (dw-metrics/mk-meter category metric))))
    (testing "returns a meter - with tags with stringified keys"
      (is (instance? Meter (dw-metrics/mk-meter category metric tags))))))

(deftest mk-histogram-test
  (let [category     "category"
        metric       "metric2"
        service-name (:app-name (ziggurat-config))
        tags         {:actor service-name
                      :foo   "bar"}]
    (testing "returns a histogram"
      (is (instance? Histogram (dw-metrics/mk-histogram category metric))))
    (testing "returns a histogram - with additional-tags"
      (is (instance? Histogram (dw-metrics/mk-histogram category metric tags))))))

(deftest initialize-test
  (let [statsd-config {:host "localhost" :port 8125 :enabled true}]
    (testing "It initializes the metrics transport and reporter and returns a map when enabled"
      (let [_ (dw-metrics/initialize statsd-config)
            {:keys [transport reporter]} @dw-metrics/reporter-and-transport-state]
        (is (instance? UdpTransport transport))
        (is (instance? DatadogReporter reporter))
        (dw-metrics/terminate)))
    (testing "It does not initialize the transport and reporter when disabled"
      (let [statsd-config (assoc statsd-config :enabled false)
            _ (dw-metrics/initialize statsd-config)
            result @dw-metrics/reporter-and-transport-state]
        (is (nil? result))
        (dw-metrics/terminate)))))
