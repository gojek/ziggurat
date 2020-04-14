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

(def default-tags {:env   "dev"
                   :actor "application_name"})

(deftest increment-count-test
  (let [passed-metric-name         "metric3"
        expected-topic-entity-name "expected-topic-entity-name"
        input-tags                 {:topic_name expected-topic-entity-name}
        expected-n                 1]
    (testing "calls update-counter with correct args - vector as an argument"
      (let [metric-namespaces  [expected-topic-entity-name "metric-ns"]
            expected-namespace (str expected-topic-entity-name ".metric-ns")
            expected-tags      default-tags]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespaces passed-metric-name expected-n input-tags))))
    (testing "calls update-counter with correct args - 3rd argument is a number"
      (let [metric-namespaces  ["metric" "ns"]
            expected-namespace "metric.ns"
            expected-tags      default-tags]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespaces passed-metric-name expected-n))))
    (testing "calls update-counter with correct args - 3rd argument is a map"
      (let [metric-namespaces  ["metric" "ns"]
            expected-namespace "metric.ns"
            expected-tags      default-tags]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespaces passed-metric-name expected-tags))))
    (testing "calls update-counter with correct args - string as an argument"
      (let [metric-namespace         "metric-ns"
            actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
            passed-tags              (merge input-tags default-tags)
            metric-namespaces-called (atom [])]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                        (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                  (is (= tags passed-tags))
                                                  (is (= metric passed-metric-name))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespace passed-metric-name 1 passed-tags)
          (is (some #{metric-namespace} @metric-namespaces-called))
          (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
    (testing "calls update-counter with correct args - w/o additional-tags argument"
      (let [metric-namespaces  [expected-topic-entity-name "metric-ns"]
            expected-namespace (str expected-topic-entity-name ".metric-ns")
            expected-tags      default-tags]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespaces passed-metric-name))))
    (testing "calls update-counter with correct args - additional-tags is nil"
      (let [metric-namespace         "metric-ns"
            actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
            metric-namespaces-called (atom [])
            expected-tags            default-tags]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                        (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                  (is (= tags expected-tags))
                                                  (is (= metric passed-metric-name))
                                                  (is (= sign +))
                                                  (is (= value expected-n)))]
          (metrics/increment-count metric-namespace passed-metric-name 1 nil)
          (is (some #{metric-namespace} @metric-namespaces-called))
          (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
    (testing "-incrementCount calls increment-count with the correct arguments"
      (let [metric-namespace        "namespace"
            increment-count-called? (atom false)]
        (with-redefs [metrics/increment-count (fn [actual-metric-namespace actual-metric]
                                                (if (and (= actual-metric-namespace metric-namespace)
                                                         (= actual-metric passed-metric-name))
                                                  (reset! increment-count-called? true)))]
          (metrics/-incrementCount metric-namespace passed-metric-name)
          (is (true? @increment-count-called?))))
      (let [tags                    (doto (java.util.HashMap.)
                                      (.put ":foo" "bar")
                                      (.put ":bar" "foo"))
            expected-tags           {:foo "bar" :bar "foo"}
            increment-count-called? (atom false)
            metric-namespace        "namespace"]
        (with-redefs [metrics/increment-count (fn [actual-namespace actual-metric actual-additional-tags]
                                                (if (and (= actual-namespace metric-namespace)
                                                         (= actual-metric passed-metric-name)
                                                         (= actual-additional-tags expected-tags))
                                                  (reset! increment-count-called? true)))]
          (metrics/-incrementCount metric-namespace passed-metric-name tags)
          (is (true? @increment-count-called?)))))))

(deftest decrement-count-test
  (let [expected-topic-name "expected-topic-name"
        passed-metric-name  "metric3"
        input-tags          {:topic_name expected-topic-name}
        expected-n          1]
    (testing "calls metrics library update-counter with the correct args - vector as an argument"
      (let [expected-tags              default-tags
            expected-metric-namespaces [expected-topic-name "metric-ns"]
            expected-namespace         (str expected-topic-name ".metric-ns")]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign -))
                                                  (is (= value expected-n)))]
          (metrics/decrement-count expected-metric-namespaces passed-metric-name expected-n input-tags))))
    (testing "calls metrics library update-counter with the correct args - string as an argument"
      (let [expected-tags   (merge default-tags input-tags)
            metric-namespace "metric-ns"
            actor-prefixed-metric-ns   (str (:app-name (ziggurat-config)) "." metric-namespace)
            metric-namespaces-called     (atom [])]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                        (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                  (is (= tags expected-tags))
                                                  (is (= metric passed-metric-name))
                                                  (is (= sign -))
                                                  (is (= value expected-n)))]
          (metrics/decrement-count metric-namespace passed-metric-name 1 input-tags)
          (is (some #{metric-namespace} @metric-namespaces-called))
          (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
    (testing "calls metrics library update-counter with the correct args - without topic name on the namespace"
      (let [expected-additional-tags   (merge default-tags input-tags)
            metric-namespaces ["metric" "ns"]
            expected-namespace "metric.ns"
            expected-tags     (merge default-tags input-tags)]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (is (= namespace expected-namespace))
                                                  (is (= metric passed-metric-name))
                                                  (is (= tags expected-tags))
                                                  (is (= sign -))
                                                  (is (= value expected-n)))]
          (metrics/decrement-count metric-namespaces passed-metric-name expected-n input-tags))))
    (testing "calls metrics library update-counter with the correct args - additional-tags is nil"
      (let [expected-tags  default-tags
            metric-namespace "metric-ns"
            actor-prefixed-metric-ns  (str (:app-name (ziggurat-config)) "." metric-namespace)
            metric-namespaces-called  (atom [])]
        (with-redefs [dw-metrics/update-counter (fn [namespace metric tags sign value]
                                                  (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                        (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                  (is (= tags expected-tags))
                                                  (is (= metric passed-metric-name))
                                                  (is (= sign -))
                                                  (is (= value expected-n)))]
          (metrics/decrement-count metric-namespace passed-metric-name 1 nil)
          (is (some #{metric-namespace} @metric-namespaces-called))
          (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
    (testing "-decrementCount passes the correct arguments to decrement-count"
      (let [metric-namespace        "namespace"
            decrement-count-called? (atom false)]
        (with-redefs [metrics/decrement-count (fn [actual-metric-namespace actual-metric]
                                                (if (and (= actual-metric-namespace metric-namespace)
                                                         (= actual-metric passed-metric-name))
                                                  (reset! decrement-count-called? true)))]
          (metrics/-decrementCount metric-namespace passed-metric-name)
          (is (true? @decrement-count-called?))))
      (let [tags          (doto (java.util.HashMap.)
                            (.put ":foo" "bar")
                            (.put ":bar" "foo"))
            expected-tags {:foo "bar" :bar "foo"}
            decrement-count-called?  (atom false)
            metric-namespace         "namespace"]
        (with-redefs [metrics/decrement-count (fn [actual-namespace actual-metric actual-additional-tags]
                                                (if (and (= actual-namespace metric-namespace)
                                                         (= actual-metric passed-metric-name)
                                                         (= actual-additional-tags expected-tags))
                                                  (reset! decrement-count-called? true)))]
          (metrics/-decrementCount metric-namespace passed-metric-name tags)
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
          report-histogram-called?  (atom false)]
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
          report-histogram-called?  (atom false)]
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
    (let [namespaces      [["multi" "ns" "test"] ["multi-ns"]]
          time-val        123
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