(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir)))

(use-fixtures :once fix/mount-only-config)

(deftest mk-meter-test
  (let [category     "category"
        metric       "metric1"
        service-name (:app-name (ziggurat-config))]
    (testing "returns a meter"
      (let [expected-tags {"actor" service-name}]
        (with-redefs [metrics/get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Meter (metrics/mk-meter category metric))))))
    (testing "returns a meter - with additional-tags"
      (let [additional-tags {:foo "bar"}
            expected-tags   (merge {"actor" service-name} (stringify-keys additional-tags))]
        (with-redefs [metrics/get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Meter (metrics/mk-meter category metric additional-tags))))))))

(deftest mk-histogram-test
  (let [category     "category"
        metric       "metric2"
        service-name (:app-name (ziggurat-config))]
    (testing "returns a histogram"
      (let [expected-tags {"actor" service-name}]
        (with-redefs [metrics/get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Histogram (metrics/mk-histogram category metric))))))
    (testing "returns a histogram - with additional-tags"
      (let [additional-tags {:foo "bar"}
            expected-tags   (merge {"actor" service-name} (stringify-keys additional-tags))]
        (with-redefs [metrics/get-tagged-metric (fn [metric-name tags]
                                                  (is (= tags expected-tags))
                                                  (.tagged metric-name tags))]
          (is (instance? Histogram (metrics/mk-histogram category metric additional-tags))))))))

(deftest increment-count-test
  (let [metric                     "metric3"
        expected-topic-entity-name "expected-topic-entity-name"
        input-additional-tags      {:topic_name expected-topic-entity-name}
        expected-n                 1]
    (testing "increases count on the meter - vector as an argument"
      (let [expected-metric-namespace ["metric" "ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  input-additional-tags]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespace) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - 3rd argument is a number"
      (let [expected-metric-namespace ["metric" "ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  {}]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-n)
          (is (= expected-n (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespace) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - 3rd argument is a map"
      (let [expected-metric-namespace ["metric" "ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  {}]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespace) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - string as an argument"
      (let [expected-metric-namespaces "metric-ns"
            mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-additional-tags   input-additional-tags]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= expected-metric-namespaces (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - w/o additional-tags argument"
      (let [expected-metric-namespace "metric-ns"
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  {}]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric)
          (is (= expected-n (.getCount meter)))
          (is (= expected-metric-namespace (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter when additional-tags is nil"
      (let [expected-metric-namespace "metric-ns"
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  {}]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-n expected-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= expected-metric-namespace (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "-incrementCount calls increment-count with the correct arguments"
      (let [metric-namespace "namespace"
            increment-count-called? (atom false)]
        (with-redefs [metrics/increment-count (fn [actual-metric-namespace actual-metric]
                                                (if (and (= actual-metric-namespace metric-namespace)
                                                         (= actual-metric metric))
                                                  (reset! increment-count-called? true)))]
          (metrics/-incrementCount metric-namespace metric)
          (is (true? @increment-count-called?))))
      (let [additional-tags (doto (java.util.HashMap.)
                              (.put ":foo" "bar")
                              (.put ":bar" "foo"))
            expected-additional-tags {:foo "bar"
                                      :bar "foo"}
            increment-count-called? (atom false)
            metric-namespace "namespace"]
        (with-redefs [metrics/increment-count (fn [actual-namespace actual-metric actual-additional-tags]
                                                (if (and (= actual-namespace metric-namespace)
                                                         (= actual-metric metric)
                                                         (= actual-additional-tags expected-additional-tags))
                                                  (reset! increment-count-called? true)))]
          (metrics/-incrementCount metric-namespace metric additional-tags)
          (is (true? @increment-count-called?)))))))

(deftest decrement-count-test
  (let [expected-topic-name   "expected-topic-name"
        metric                "metric3"
        mk-meter-args         (atom nil)
        meter                 (Meter.)
        input-additional-tags {:topic_name expected-topic-name}
        expected-n            1]
    (testing "decreases count on the meter - vector as an argument"
      (let [expected-additional-tags  input-additional-tags
            expected-metric-namespace ["metric" "ns"]]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespace metric expected-n input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespace) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter - string as an argument"
      (let [expected-additional-tags   input-additional-tags
            expected-metric-namespaces "metric-ns"]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= expected-metric-namespaces (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter - without topic name on the namespace"
      (let [expected-additional-tags   input-additional-tags
            expected-metric-namespaces ["metric" "ns"]]
        (with-redefs [metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespaces) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter when additional-tags is nil"
      (let [expected-additional-tags  {}
            expected-metric-namespace "metric-ns"]
        (with-redefs [metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric expected-n expected-additional-tags)
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespace metric expected-n expected-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= expected-metric-namespace (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "-decrementCount passes the correct arguments to decrement-count"
      (let [metric-namespace "namespace"
            decrement-count-called? (atom false)]
        (with-redefs [metrics/decrement-count (fn [actual-metric-namespace actual-metric]
                                                (if (and (= actual-metric-namespace metric-namespace)
                                                         (= actual-metric metric))
                                                  (reset! decrement-count-called? true)))]
          (metrics/-decrementCount metric-namespace metric)
          (is (true? @decrement-count-called?))))
      (let [additional-tags (doto (java.util.HashMap.)
                              (.put ":foo" "bar")
                              (.put ":bar" "foo"))
            expected-additional-tags {:foo "bar"
                                      :bar "foo"}
            decrement-count-called? (atom false)
            metric-namespace "namespace"]
        (with-redefs [metrics/decrement-count (fn [actual-namespace actual-metric actual-additional-tags]
                                                (if (and (= actual-namespace metric-namespace)
                                                         (= actual-metric metric)
                                                         (= actual-additional-tags expected-additional-tags))
                                                  (reset! decrement-count-called? true)))]
          (metrics/-decrementCount metric-namespace metric additional-tags)
          (is (true? @decrement-count-called?)))))))

(deftest report-histogram-test
  (let [expected-topic-entity-name "expected-topic-entity-name"
        input-additional-tags      {:topic_name expected-topic-entity-name}
        time-val                   10]
    (testing "updates time-val - vector as an argument"
      (let [expected-metric-namespace ["message-received-delay-histogram" "ns"]
            mk-histogram-args         (atom nil)
            reservoir                 (UniformReservoir.)
            histogram                 (Histogram. reservoir)
            expected-additional-tags  input-additional-tags]
        (with-redefs [metrics/mk-histogram (fn [metric-namespace metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (reset! mk-histogram-args {:metric-namespace metric-namespace
                                                                        :metric           metric})
                                             histogram)]
          (metrics/report-histogram expected-metric-namespace time-val input-additional-tags)
          (is (= 1 (.getCount histogram)))
          (is (= (metrics/intercalate-dot expected-metric-namespace) (:metric-namespace @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args))))))
    (testing "updates time-val - string as an argument"
      (let [expected-metric-namespaces "message-received-delay-histogram"
            mk-histogram-args          (atom nil)
            reservoir                  (UniformReservoir.)
            histogram                  (Histogram. reservoir)
            expected-additional-tags   input-additional-tags]
        (with-redefs [metrics/mk-histogram (fn [metric-namespaces metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (reset! mk-histogram-args {:metric-namespaces metric-namespaces
                                                                        :metric            metric})
                                             histogram)]
          (metrics/report-histogram expected-metric-namespaces time-val input-additional-tags)
          (is (= 1 (.getCount histogram)))
          (is (= expected-metric-namespaces (:metric-namespaces @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args))))))
    (testing "updates time-val - w/o additional-tags argument"
      (let [expected-metric-namespace "message-received-delay-histogram"
            mk-histogram-args         (atom nil)
            reservoir                 (UniformReservoir.)
            histogram                 (Histogram. reservoir)
            expected-additional-tags  nil]
        (with-redefs [metrics/mk-histogram (fn [metric-namespace metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (reset! mk-histogram-args {:metric-namespace metric-namespace
                                                                        :metric           metric})
                                             histogram)]
          (metrics/report-histogram expected-metric-namespace time-val)
          (is (= 1 (.getCount histogram)))
          (is (= expected-metric-namespace (:metric-namespace @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args)))))))
  (testing "report time java function passes the correct parameters to report time"
    (let [expected-metric-namespace "namespace"
          expected-time-val         123
          report-histogram-called?       (atom false)]
      (with-redefs [metrics/report-histogram (fn [actual-metric-namespace actual-time-val]
                                               (if (and (= actual-metric-namespace expected-metric-namespace)
                                                        (= actual-time-val expected-time-val))
                                                 (reset! report-histogram-called? true)))]
        (metrics/-reportTime expected-metric-namespace expected-time-val)
        (is (true? @report-histogram-called?))))
    (let [expected-metric-namespace "namespace"
          expected-time-val         123
          additional-tags           (doto (java.util.HashMap.)
                                      (.put ":foo" "bar")
                                      (.put ":bar" "foo"))
          expected-additional-tags {:foo "bar" :bar "foo"}
          report-histogram-called?       (atom false)]
      (with-redefs [metrics/report-histogram (fn [actual-metric-namespace actual-time-val actual-additional-tags]
                                               (if (and (= actual-metric-namespace expected-metric-namespace)
                                                        (= actual-time-val expected-time-val)
                                                        (= actual-additional-tags expected-additional-tags))
                                                 (reset! report-histogram-called? true)))]
        (metrics/-reportTime expected-metric-namespace expected-time-val additional-tags)
        (is (true? @report-histogram-called?))))))
