(ns ziggurat.prometheus-exporter-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.prometheus-exporter :as prometheus-exporter]
            [iapetos.core :as prometheus]))

(defn clear-prometheus-registry []
  (reset! prometheus-exporter/registry (prometheus/collector-registry)))

(deftest test-register-collecter-if-not-exist
  (testing "Should register gauge if it does not exist"
    (clear-prometheus-registry)
    (let [label {:name "test-gauge" :namespace "bar"}]
      (is (nil? (@prometheus-exporter/registry label)) "Gauge should not exist initially")
      (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge label)
      (is (not (nil? (@prometheus-exporter/registry label))) "Gauge should be registered in the registry"))

    (testing "Gauge not registered if it exists"
      (clear-prometheus-registry)
      (let [label "test-cauge"
            existing-cauge (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge label)]
        (let [expected-nil (prometheus-exporter/register-collecter-if-not-exist prometheus/gauge label)]
          (is (not (nil? existing-cauge)))
          (is (nil? expected-nil) "Should retrieve the existing cauge"))))))

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
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)))]
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
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)))]
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
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)))]
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
      (let [gauge-value (prometheus-exporter/get-metric-value (keyword (str namespace "/" metric)))]
        (is (= (gauge-value :count) 2.0))))))

  