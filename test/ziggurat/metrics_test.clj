(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.dropwizard-metrics-wrapper :as dw-metrics]
            [clojure.tools.logging :as log])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir MetricRegistry)))

(use-fixtures :once fix/mount-only-config)

(def default-tags {:env "dev"
                   :actor "application_name"})

(deftest increment-count-test
  (let [metric                     "metric3"
        expected-topic-entity-name "expected-topic-entity-name"
        input-additional-tags      {:topic_name expected-topic-entity-name}
        expected-n                 1]
    (testing "increases count on the meter - vector as an argument"
      (let [expected-metric-namespaces [expected-topic-entity-name "metric-ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  default-tags]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespaces) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - 3rd argument is a number"
      (let [expected-metric-namespace ["metric" "ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  default-tags]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespace metric additional-tags]
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
            expected-additional-tags  default-tags]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespace metric additional-tags]
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
            actor-prefixed-metric-ns   (str (:app-name (ziggurat-config)) "." expected-metric-namespaces)
            mk-meter-args              (atom nil)
            meter                      (Meter.)
            expected-additional-tags   (merge input-additional-tags default-tags)
            expected-n                 2]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (or (and (= metric-namespaces expected-metric-namespaces) (= 0 (.getCount meter)))
                                                 (and (= metric-namespaces actor-prefixed-metric-ns) (= 1 (.getCount meter)))))
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric 1 input-additional-tags)
          (is (= expected-n (.getCount meter)))
          (is (= actor-prefixed-metric-ns (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter - w/o additional-tags argument"
      (let [expected-metric-namespaces [expected-topic-entity-name "metric-ns"]
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  default-tags]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespaces
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespaces metric)
          (is (= expected-n (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespaces) (:metric-namespace @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "increases count on the meter when additional-tags is nil"
      (let [expected-metric-namespace "metric-ns"
            actor-prefixed-metric-ns  (str (:app-name (ziggurat-config)) "." expected-metric-namespace)
            mk-meter-args             (atom nil)
            meter                     (Meter.)
            expected-additional-tags  default-tags
            expected-n                2]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (or (and (= metric-namespace expected-metric-namespace) (= 0 (.getCount meter)))
                                                 (and (= metric-namespace actor-prefixed-metric-ns) (= 1 (.getCount meter)))))
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (metrics/increment-count expected-metric-namespace metric 1 nil)
          (is (= expected-n (.getCount meter)))
          (is (= actor-prefixed-metric-ns (:metric-namespace @mk-meter-args)))
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
            expected-additional-tags {:foo "bar" :bar "foo"}
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
      (let [expected-additional-tags   default-tags
            expected-metric-namespaces [expected-topic-name "metric-ns"]
            meter                      (Meter.)
            _                          (.mark meter expected-n)]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric           metric})
                                         meter)]
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespaces) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter - string as an argument"
      (let [expected-additional-tags   (merge default-tags input-additional-tags)
            expected-metric-namespaces "metric-ns"
            actor-prefixed-metric-ns   (str (:app-name (ziggurat-config)) "." expected-metric-namespaces)
            expected-n                 2
            meter                      (Meter.)
            _                          (.mark meter expected-n)]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (is (or (and (= metric-namespaces expected-metric-namespaces) (= 2 (.getCount meter)))
                                                 (and (= metric-namespaces actor-prefixed-metric-ns) (= 1 (.getCount meter)))))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric 1 input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= actor-prefixed-metric-ns (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter - without topic name on the namespace"
      (let [expected-additional-tags   (merge default-tags input-additional-tags)
            expected-metric-namespaces ["metric" "ns"]
            meter                      (Meter.)
            _                          (.mark meter expected-n)]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespaces metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (reset! mk-meter-args {:metric-namespaces metric-namespaces
                                                                :metric            metric})
                                         meter)]
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespaces metric expected-n input-additional-tags)
          (is (zero? (.getCount meter)))
          (is (= (metrics/intercalate-dot expected-metric-namespaces) (:metric-namespaces @mk-meter-args)))
          (is (= metric (:metric @mk-meter-args))))))
    (testing "decreases count on the meter when additional-tags is nil"
      (let [expected-additional-tags  default-tags
            expected-metric-namespace "metric-ns"
            actor-prefixed-metric-ns  (str (:app-name (ziggurat-config)) "." expected-metric-namespace)
            expected-n                2
            meter                     (Meter.)
            _                         (.mark meter expected-n)]
        (with-redefs [dw-metrics/mk-meter (fn [metric-namespace metric additional-tags]
                                         (is (= additional-tags expected-additional-tags))
                                         (is (or (and (= metric-namespace expected-metric-namespace) (= 2 (.getCount meter)))
                                                 (and (= metric-namespace actor-prefixed-metric-ns) (= 1 (.getCount meter)))))
                                         (reset! mk-meter-args {:metric-namespace metric-namespace
                                                                :metric           metric})
                                         meter)]
          (is (= expected-n (.getCount meter)))
          (metrics/decrement-count expected-metric-namespace metric 1 nil)
          (is (zero? (.getCount meter)))
          (is (= actor-prefixed-metric-ns (:metric-namespace @mk-meter-args)))
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
            expected-additional-tags {:foo "bar" :bar "foo"}
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
      (let [expected-metric-namespace [expected-topic-entity-name "message-received-delay-histogram"]
            mk-histogram-args         (atom nil)
            reservoir                 (UniformReservoir.)
            histogram                 (Histogram. reservoir)
            expected-additional-tags  default-tags]
        (with-redefs [dw-metrics/mk-histogram (fn [metric-namespace metric additional-tags]
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
            actor-prefixed-metric-ns   (str (:app-name (ziggurat-config)) "." expected-metric-namespaces)
            mk-histogram-args          (atom nil)
            reservoir                  (UniformReservoir.)
            histogram                  (Histogram. reservoir)
            expected-additional-tags   (merge default-tags input-additional-tags)
            expected-n                 2]
        (with-redefs [dw-metrics/mk-histogram (fn [metric-namespaces metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (is (or (and (= metric-namespaces expected-metric-namespaces) (= 0 (.getCount histogram)))
                                                     (and (= metric-namespaces actor-prefixed-metric-ns) (= 1 (.getCount histogram)))))
                                             (reset! mk-histogram-args {:metric-namespaces metric-namespaces
                                                                        :metric            metric})
                                             histogram)]
          (metrics/report-histogram expected-metric-namespaces time-val input-additional-tags)
          (is (= expected-n (.getCount histogram)))
          (is (= actor-prefixed-metric-ns (:metric-namespaces @mk-histogram-args)))
          (is (= "all" (:metric @mk-histogram-args))))))
    (testing "updates time-val - w/o additional-tags argument"
      (let [expected-metric-namespace "message-received-delay-histogram"
            mk-histogram-args         (atom nil)
            actor-prefixed-metric-ns  (str (:app-name (ziggurat-config)) "." expected-metric-namespace)
            reservoir                 (UniformReservoir.)
            histogram                 (Histogram. reservoir)
            expected-additional-tags  default-tags
            expected-n                2]
        (with-redefs [dw-metrics/mk-histogram (fn [metric-namespace metric additional-tags]
                                             (is (= additional-tags expected-additional-tags))
                                             (is (or (and (= metric-namespace expected-metric-namespace) (= 0 (.getCount histogram)))
                                                     (and (= metric-namespace actor-prefixed-metric-ns) (= 1 (.getCount histogram)))))
                                             (reset! mk-histogram-args {:metric-namespace metric-namespace
                                                                        :metric           metric})
                                             histogram)]
          (metrics/report-histogram expected-metric-namespace time-val)
          (is (= expected-n (.getCount histogram)))
          (is (= actor-prefixed-metric-ns (:metric-namespace @mk-histogram-args)))
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
          expected-additional-tags  {:foo "bar" :bar "foo"}
          report-histogram-called?       (atom false)]
      (with-redefs [metrics/report-histogram (fn [actual-metric-namespace actual-time-val actual-additional-tags]
                                               (is (= actual-additional-tags expected-additional-tags))
                                               (if (and (= actual-metric-namespace expected-metric-namespace)
                                                        (= actual-time-val expected-time-val)
                                                        (= actual-additional-tags expected-additional-tags))
                                                 (reset! report-histogram-called? true)))]
        (metrics/-reportTime expected-metric-namespace expected-time-val additional-tags)
        (is (true? @report-histogram-called?))))))

(deftest report-time-test
  (let [metric-namespace "metric-namespace"
        value            12
        additional-tags  {:foo "bar"}]
    (testing "report time passes the correct metric-namespace and values to report-histogram and logs deprecation notice"
      (with-redefs [metrics/report-histogram (fn [received-metric-ns received-val]
                                               (is (= metric-namespace received-metric-ns))
                                               (is (= value received-val)))]
        (metrics/report-time metric-namespace value)))
    (testing "report time passes the correct namespace, val and addition-tags to report-histogram and logs deprecation notice"
      (with-redefs [metrics/report-histogram (fn [received-metric-ns received-val received-additional-tags]
                                               (is (= metric-namespace received-metric-ns))
                                               (is (= value received-val))
                                               (is (= additional-tags received-additional-tags)))]
        (metrics/report-time metric-namespace value additional-tags)))))

(deftest multi-ns-report-time-test
  (testing "calls multi-ns-report-histogram with the correct arguments"
    (let [namespaces [["multi" "ns" "test"] ["multi-ns"]]
          time-val   123
          additional-tags {:foo "bar"}]
      (with-redefs [metrics/multi-ns-report-histogram (fn [received-nss received-time-val received-additional-tags]
                                                        (is (= received-nss namespaces))
                                                        (is (= received-time-val time-val))
                                                        (is (= received-additional-tags additional-tags)))]
        (metrics/multi-ns-report-time namespaces time-val additional-tags)))))

(deftest multi-ns-increment-count-test
  (testing "multi-ns-increment-count calls increment-count for every namespace list passed"
    (let [metric-namespaces-list               [["test" "multi" "ns"] ["test-ns"]]
          expected-metric                      "test-metric"
          expected-additional-tags             (merge default-tags {:foo "bar"})
          increment-count-call-counts          (atom 0)
          expected-increment-count-call-counts 2]
      (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                              (when (and (some #{metric-namespaces} metric-namespaces-list)
                                                         (= metric expected-metric)
                                                         (= additional-tags expected-additional-tags))
                                                (swap! increment-count-call-counts inc)))]
        (metrics/multi-ns-increment-count metric-namespaces-list expected-metric expected-additional-tags)
        (is (= expected-increment-count-call-counts @increment-count-call-counts))))))

(deftest multi-ns-report-histogram-test
  (testing "multi-ns-report-histogram calls report-histogram for every namespace list passed"
    (let [metric-namespaces-list                [["test" "multi" "ns"] ["test-ns"]]
          expected-metric                       "test-metric"
          expected-additional-tags              (merge default-tags {:foo "bar"})
          report-histogram-call-counts          (atom 0)
          expected-report-histogram-call-counts 2]
      (with-redefs [metrics/report-histogram (fn [metric-namespaces metric additional-tags]
                                               (when (and (some #{metric-namespaces} metric-namespaces-list)
                                                          (= metric expected-metric)
                                                          (= additional-tags expected-additional-tags))
                                                 (swap! report-histogram-call-counts inc)))]
        (metrics/multi-ns-report-histogram metric-namespaces-list expected-metric expected-additional-tags)
        (is (= expected-report-histogram-call-counts @report-histogram-call-counts))))))