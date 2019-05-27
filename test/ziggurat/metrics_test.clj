(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [ziggurat.metrics :as metrics])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir)))

(deftest mk-meter-test
  (testing "returns a meter"
    (let [category "category"
          metric   "metric1"
          meter    (metrics/mk-meter category metric)]
      (is (instance? Meter meter)))))

(deftest mk-histogram-test
  (testing "returns a histogram"
    (let [category "category"
          metric   "metric2"
          meter    (metrics/mk-histogram category metric)]
      (is (instance? Histogram meter)))))

(deftest increment-count-test
  (let [metric-ns ["metric-ns"]
        metric    "metric3"]
    (testing "increases count on the meter"
      (let [mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-topic-entity-name "expected-topic-entity-name"]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric topic-entity-name]
                                         (is (= topic-entity-name expected-topic-entity-name))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count metric-ns metric expected-topic-entity-name)
          (is (= 1 (.getCount meter)))
          (is (= (apply str (interpose "." metric-ns)) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter when topic-entity-name is nil"
      (let [mk-meter-args            (atom nil)
            meter                    (Meter.)
            expected-additional-tags nil]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count metric-ns metric expected-additional-tags)
          (is (= 1 (.getCount meter)))
          (is (= (apply str (interpose "." metric-ns)) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))))

(deftest decrement-count-test
  (let [metric-ns     ["metric-ns"]
        metric        "metric3"
        mk-meter-args (atom nil)
        meter         (Meter.)]
    (testing "decreases count on the meter"
      (let [expected-additional-tags {:topic-name "expected-topic-name"}]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count metric-ns metric expected-additional-tags)
          (is (= 1 (.getCount meter)))
          (metrics/decrement-count metric-ns metric expected-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (apply str (interpose "." metric-ns)) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter when additional-tags is nil"
      (let [expected-additional-tags nil]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count metric-ns metric expected-additional-tags)
          (is (= 1 (.getCount meter)))
          (metrics/decrement-count metric-ns metric expected-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (apply str (interpose "." metric-ns)) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))))

(deftest report-time-test
  (testing "updates time-val"
    (let [metric-ns                  ["message-received-delay-histogram"]
          time-val                   10
          mk-histogram-args          (atom nil)
          reservoir                  (UniformReservoir.)
          histogram                  (Histogram. reservoir)
          expected-topic-entity-name "expected-topic-entity-name"]
      (with-redefs [metrics/mk-histogram (fn [metric-ns metric topic-entity-name]
                                           (is (= topic-entity-name expected-topic-entity-name))
                                           (reset! mk-histogram-args {:metric-namespace metric-ns
                                                                      :metric           metric})
                                           histogram)]
        (metrics/report-time metric-ns time-val expected-topic-entity-name)
        (is (= 1 (.getCount histogram)))
        (is (= (apply str (interpose "." metric-ns)) (:metric-namespace @mk-histogram-args)))
        (is (= "all" (:metric @mk-histogram-args)))))))
