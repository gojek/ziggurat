(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [ziggurat.metrics :as metrics])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir)))

(deftest mk-meter-test
  (testing "returns a meter"
    (let [category "category"
          metric   "metric1"
          meter    (metrics/mk-meter category metric {:actor "service"})]
      (is (instance? Meter meter)))))

(deftest mk-histogram-test
  (testing "returns a histogram"
    (let [category "category"
          metric   "metric2"
          meter    (metrics/mk-histogram category metric {:actor "service"})]
      (is (instance? Histogram meter)))))

(deftest increment-count-test
  (let [metric                     "metric3"
        expected-topic-entity-name "expected-topic-entity-name"
        input-additional-tags      {:topic_name expected-topic-entity-name}]
    (testing "increases count on the meter"
      (let [expected-metric-namespaces [expected-topic-entity-name "metric-ns"]
            mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-additional-tags   {}]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric input-additional-tags)
          (is (= 1 (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - without topic name on the namespace"
      (let [expected-metric-namespaces ["metric-ns"]
            mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-additional-tags   input-additional-tags]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric input-additional-tags)
          (is (= 1 (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter when additional-tags is nil"
      (let [expected-metric-namespaces [expected-topic-entity-name "metric-ns"]
            mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-additional-tags   nil]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-additional-tags)
          (is (= 1 (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))))

(deftest decrement-count-test
  (let [expected-topic-name   "expected-topic-name"
        metric                "metric3"
        mk-meter-args         (atom nil)
        meter                 (Meter.)
        input-additional-tags {:topic_name expected-topic-name}]
    (testing "decreases count on the meter"
      (let [expected-additional-tags   {}
            expected-metric-namespaces [expected-topic-name "metric-ns"]]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric input-additional-tags)
          (is (= 1 (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter - without topic name on the namespace"
      (let [expected-additional-tags   input-additional-tags
            expected-metric-namespaces ["metric-ns"]]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric input-additional-tags)
          (is (= 1 (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter when additional-tags is nil"
      (let [expected-additional-tags   nil
            expected-metric-namespaces [expected-topic-name "metric-ns"]]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-additional-tags)
          (is (= 1 (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric expected-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))))

(deftest report-time-test
  (let [expected-topic-entity-name "expected-topic-entity-name"
        input-additional-tags      {:topic_name expected-topic-entity-name}
        time-val                   10]
    (testing "updates time-val"
      (let [expected-metric-namespaces [expected-topic-entity-name "message-received-delay-histogram"]
            mk-histogram-args          (atom nil)
            reservoir                  (UniformReservoir.)
            histogram                  (Histogram. reservoir)
            expected-additional-tags   {}]
        (with-redefs [metrics/mk-histogram (fn [metric-namespaces metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (reset! mk-histogram-args {:metric-namespaces metric-namespaces
                                                                        :metric            metric})
                                             histogram)]
          (metrics/report-time expected-metric-namespaces time-val input-additional-tags)
          (is (= 1 (.getCount histogram)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args))))))
    (testing "updates time-val - without topic name on the namespace"
      (let [expected-metric-namespaces ["message-received-delay-histogram"]
            mk-histogram-args          (atom nil)
            reservoir                  (UniformReservoir.)
            histogram                  (Histogram. reservoir)
            expected-additional-tags   input-additional-tags]
        (with-redefs [metrics/mk-histogram (fn [metric-namespaces metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (reset! mk-histogram-args {:metric-namespaces metric-namespaces
                                                                        :metric            metric})
                                             histogram)]
          (metrics/report-time expected-metric-namespaces time-val input-additional-tags)
          (is (= 1 (.getCount histogram)))
          (is (= (apply str (interpose "." expected-metric-namespaces)) (:metric-namespaces @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args))))))))
