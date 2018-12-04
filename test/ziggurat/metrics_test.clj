(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [ziggurat.metrics :as metrics])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir)))

(deftest mk-meter-test
  (testing "returns a meter"
    (let [category "category"
          metric "metric1"
          meter (metrics/mk-meter category metric)]
      (is (instance? Meter meter)))))

(deftest mk-histogram-test
  (testing "returns a histogram"
    (let [category "category"
          metric "metric2"
          meter (metrics/mk-histogram category metric)]
      (is (instance? Histogram meter)))))


(deftest increment-count-test
  (testing "increases count on the meter"
    (let [metric-ns "metric-ns"
          metric "metric3"
          mk-meter-args (atom nil)
          meter (Meter.)]
      (with-redefs [metrics/mk-meter (fn [metric-namespace metric]
                                       (reset! mk-meter-args {:metric-namespace metric-namespace
                                                              :metric           metric})
                                       meter)]
        (metrics/increment-count metric-ns metric)
        (is (= 1 (.getCount meter)))
        (is (= metric-ns (:metric-namespace @mk-meter-args)))
        (is (= metric (:metric @mk-meter-args)))))))

(deftest decrement-count-test
  (testing "decreases count on the meter"
    (let [metric-ns "metric-ns"
          metric "metric3"
          mk-meter-args (atom nil)
          meter (Meter.)]
      (with-redefs [metrics/mk-meter (fn [metric-namespace metric]
                                       (reset! mk-meter-args {:metric-namespace metric-namespace
                                                              :metric           metric})
                                       meter)]
        (metrics/increment-count metric-ns metric)
        (is (= 1 (.getCount meter)))
        (metrics/decrement-count metric-ns metric)
        (is (= 0 (.getCount meter)))
        (is (= metric-ns (:metric-namespace @mk-meter-args)))
        (is (= metric (:metric @mk-meter-args)))))))

(deftest report-time-test
  (testing "updates time-val"
    (let [metric-ns "metric-ns"
          time-val 10
          mk-histogram-args (atom nil)
          reservoir (UniformReservoir.)
          histogram (Histogram. reservoir)]
      (with-redefs [metrics/mk-histogram (fn [metric-ns metric]
                                           (reset! mk-histogram-args {:metric-namespace metric-ns
                                                                      :metric           metric})
                                           histogram)]
        (metrics/report-time metric-ns time-val)
        (is (= 1 (.getCount histogram)))
        (is (= metric-ns (:metric-namespace @mk-histogram-args)))
        (is (= "all" (:metric @mk-histogram-args)))))))
