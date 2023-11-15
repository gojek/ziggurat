(ns ziggurat.prometheus-exporter-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.prometheus-exporter :as prometheus-exporter]
            [iapetos.core :as prometheus]))

(defn clear-prometheus-registry []
  (reset! prometheus-exporter/registry (prometheus/collector-registry)))

(deftest test-register-collecter-if-not-exist
  (testing "Should register gauge if it does not exist"
    (clear-prometheus-registry)
    (let [tags {:actor-topic "food" :app_name "delivery"}
          metric-name {:name "test-gauge" :namespace "bar"}
          label-vec (vec (keys tags))]
      (is (nil? (@prometheus-exporter/registry metric-name)) "Gauge should not exist initially")
      (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge metric-name label-vec)
      (is (not (nil? (@prometheus-exporter/registry metric-name))) "Gauge should be registered in the registry"))

    (testing "Gauge not registered if it exists"
      (clear-prometheus-registry)
      (let [tags {:actor-topic "food" :app_name "delivery"}
            metric-name "test-gauge"
            label-vec (vec (keys tags))
            existing-gauge (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge metric-name label-vec)]
        (let [expected-nil (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge metric-name label-vec)]
          (is (not (nil? existing-gauge)))
          (is (nil? expected-nil) "Should retrieve the existing gauge"))))))

(deftest test-update-counter
  (testing "Updating counter can increments its value"
    (clear-prometheus-registry)
    (let [namespace "test_namespace"
          metric "test_metric"
          old_value 5.0
          increment 2
          new-value (+ old_value increment)
          tags {:some "label"}
          _ (prometheus-exporter/update-counter namespace metric tags old_value)]
      (prometheus-exporter/update-counter namespace metric tags increment)
      (@prometheus-exporter/registry (assoc tags :name metric :namespace namespace))
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)) tags)]
        (is (= new-value gauge-value) "Gauge value should be incremented"))))

  (testing "Updating counter can decrement its value"
    (clear-prometheus-registry)
    (let [namespace "test_namespace"
          metric "test_metric"
          old_value 5.0
          decrement -2
          new-value (+ old_value decrement)
          tags {:some "label"}
          _ (prometheus-exporter/update-counter namespace metric tags old_value)]
      (prometheus-exporter/update-counter namespace metric tags decrement)
      (@prometheus-exporter/registry (assoc tags :name metric :namespace namespace))
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)) tags)]
        (is (= new-value gauge-value) "Gauge value should be incremented")))))

(deftest test-report-histogram
  (testing "should register histogram when it does not exist"
    (clear-prometheus-registry)
    (let [namespace "test_namespace"
          metric "test_metric"
          value 5.0
          tags {:some "label"}
          label (assoc tags :name metric :namespace namespace)]
      (is (nil? (@prometheus-exporter/registry label)))
      (prometheus-exporter/report-histogram namespace metric tags value)
      (is (not (nil? (@prometheus-exporter/registry label))))
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)) tags)]
        (is (= (gauge-value :sum) 5.0))
        (is (= (gauge-value :count) 1.0)))))
  (testing "should update histogram when it exist and increment the count"
    (clear-prometheus-registry)
    (let [namespace "test_namespace"
          metric "test_metric"
          value 5.0
          tags {:some "label"}
          label (assoc tags :name metric :namespace namespace)
          _ (prometheus-exporter/report-histogram namespace metric tags value)]
      (is (not (nil? (@prometheus-exporter/registry label))))
      (prometheus-exporter/report-histogram namespace metric tags value)
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)) tags)]
        (is (= (gauge-value :count) 2.0))))))
