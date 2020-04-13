(ns ziggurat.dropwizard-metric-wrapper-test
  (:require [ziggurat.dropwizard-metrics-wrapper :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]])
  (:import [io.dropwizard.metrics5 Meter Histogram]))


(deftest mk-meter-test
  (let [category     "category"
        metric       "metric1"
        service-name (:app-name (ziggurat-config))]
    (testing "returns a meter"
      (let [expected-tags {"actor" service-name}]
        (with-redefs [get-tagged-metric (fn [metric-name tags]
                                          (is (= tags expected-tags))
                                          (.tagged metric-name tags))]
          (is (instance? Meter (mk-meter category metric))))))
    (testing "returns a meter - with additional-tags"
      (let [additional-tags {:foo "bar"}
            expected-tags   (merge {"actor" service-name} (stringify-keys additional-tags))]
        (with-redefs [get-tagged-metric (fn [metric-name tags]
                                          (is (= tags expected-tags))
                                          (.tagged metric-name tags))]
          (is (instance? Meter (mk-meter category metric additional-tags))))))))

(deftest mk-histogram-test
  (let [category     "category"
        metric       "metric2"
        service-name (:app-name (ziggurat-config))]
    (testing "returns a histogram"
      (let [expected-tags {"actor" service-name}]
        (with-redefs [get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Histogram (mk-histogram category metric))))))
    (testing "returns a histogram - with additional-tags"
      (let [additional-tags {:foo "bar"}
            expected-tags   (merge {"actor" service-name} (stringify-keys additional-tags))]
        (with-redefs [get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Histogram (mk-histogram category metric additional-tags))))))))

