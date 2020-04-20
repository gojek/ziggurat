(ns ziggurat.dropwizard-metrics-wrapper-test
  (:require [ziggurat.dropwizard-metrics-wrapper :refer :all :as dw-metrics]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]])
  (:import [io.dropwizard.metrics5 Meter Histogram]
           (com.gojek.metrics.datadog DatadogReporter)
           (com.gojek.metrics.datadog.transport UdpTransport)))

(deftest mk-meter-test
  (let [category     "category"
        metric       "metric1"
        service-name (:app-name (ziggurat-config))
        tags         {:actor service-name
                      :foo   "bar"}]
    (testing "returns a meter with no tags when none are passed to it"
      (let [expected-tags {}]
        (with-redefs [dw-metrics/get-tagged-metric (fn [metric-name tags]
                                                     (is (= tags expected-tags))
                                                     (.tagged metric-name tags))]
          (is (instance? Meter (mk-meter category metric))))))
    (testing "returns a meter - with tags with stringified keys"
      (let [expected-tags (stringify-keys tags)]
        (with-redefs [dw-metrics/get-tagged-metric (fn [metric-name tags]
                                                     (is (= tags expected-tags))
                                                     (.tagged metric-name tags))]
          (is (instance? Meter (mk-meter category metric tags))))))))

(deftest mk-histogram-test
  (let [category     "category"
        metric       "metric2"
        service-name (:app-name (ziggurat-config))
        tags         {:actor service-name
                      :foo   "bar"}]
    (testing "returns a histogram"
      (let [expected-tags {}]
        (with-redefs [dw-metrics/get-tagged-metric (fn [metric-name tags]
                                                     (is (= tags expected-tags))
                                                     (.tagged metric-name tags))]
          (is (instance? Histogram (mk-histogram category metric))))))
    (testing "returns a histogram - with additional-tags"
      (let [expected-tags (stringify-keys tags)]
        (with-redefs [dw-metrics/get-tagged-metric (fn [metric-name tags]
                                                     (is (= tags expected-tags))
                                                     (.tagged metric-name tags))]
          (is (instance? Histogram (mk-histogram category metric tags))))))))

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

