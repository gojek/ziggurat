(ns ziggurat.prometheus-exporter-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.prometheus-exporter :as prometheus-exporter]
            [iapetos.core :as prometheus]))

(defn clear-prometheus-registry []
  (reset! prometheus-exporter/registry (prometheus/collector-registry)))

(deftest test-register-counter-if-not-exist
  (testing "Counter registration"
    (clear-prometheus-registry)
    (let [label {:name "test-counter" :namespace "bar"}]
      (is (nil? (@prometheus-exporter/registry label)) "Counter should not exist initially")
      (prometheus-exporter/register-counter-if-not-exist label)
      (is (not (nil? (@prometheus-exporter/registry label))) "Counter should be registered in the registry"))

    (testing "Counter not registered if it exists"
      (clear-prometheus-registry)
      (let [label "test-counter"
            existing-counter (prometheus-exporter/register-counter-if-not-exist label)]
        (let [expected-nil (prometheus-exporter/register-counter-if-not-exist label)]
          (is (not (nil? existing-counter)))
          (is (nil? expected-nil) "Should retrieve the existing counter"))))))

(deftest test-update-counter
  (testing "Updating counter increments its value"
    (clear-prometheus-registry)
    (let [namespace "test_namespace"
          metric "test_metric"
          value 1.0
          tags {:some "label"}]
      (prometheus-exporter/update-counter namespace metric value tags)
      (@prometheus-exporter/registry (assoc tags :name metric :namespace namespace))
      (let [gauge-value (prometheus-exporter/get-metric-value (str namespace "_" metric))]
        (is (= value gauge-value) "Gauge value should be incremented")))))