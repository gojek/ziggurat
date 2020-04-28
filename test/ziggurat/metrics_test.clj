(ns ziggurat.metrics-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [stringify-keys]]
            [ziggurat.config :refer [ziggurat-config config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.mock-metrics-implementation :as mock-metrics]
            [clojure.tools.logging :as log]
            [mount.core :as mount]
            [ziggurat.clj-statsd-metrics-wrapper])
  (:import (io.dropwizard.metrics5 Meter Histogram UniformReservoir MetricRegistry)
           (ziggurat.dropwizard_metrics_wrapper DropwizardMetrics)
           (ziggurat.clj_statsd_metrics_wrapper CljStatsd)))

(use-fixtures :once (join-fixtures [fix/mount-only-config]))

(def default-tags {:env   "dev"
                   :actor "application_name"})

(deftest increment-count-test
  (with-redefs [config (assoc-in config [:ziggurat :metrics :constructor] "ziggurat.util.mock-metrics-implementation/->MockImpl")]
    (mount/start (mount/only [#'metrics/statsd-reporter]))
    (let [passed-metric-name         "metric3"
          expected-topic-entity-name "expected-topic-entity-name"
          input-tags                 {:topic_name expected-topic-entity-name}
          expected-n                 1]
      (testing "calls update-counter with correct args - vector as an argument"
        (let [metric-namespaces  [expected-topic-entity-name "metric-ns"]
              expected-namespace (str expected-topic-entity-name ".metric-ns")
              expected-tags      default-tags]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/increment-count metric-namespaces passed-metric-name expected-n input-tags))))
      (testing "calls update-counter with correct args - 3rd argument is a number"
        (let [metric-namespaces  ["metric" "ns"]
              expected-namespace "metric.ns"
              expected-tags      default-tags]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/increment-count metric-namespaces passed-metric-name expected-n))))
      (testing "calls update-counter with correct args - 3rd argument is a map"
        (let [metric-namespaces  ["metric" "ns"]
              expected-namespace "metric.ns"
              expected-tags      default-tags]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/increment-count metric-namespaces passed-metric-name expected-tags))))
      (testing "calls update-counter with correct args - string as an argument"
        (let [metric-namespace         "metric-ns"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              passed-tags              (merge input-tags default-tags)
              metric-namespaces-called (atom [])]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                            (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                      (is (= tags passed-tags))
                                                      (is (= metric passed-metric-name))
                                                      (is (= value expected-n)))]
            (metrics/increment-count metric-namespace passed-metric-name 1 passed-tags)
            (is (some #{metric-namespace} @metric-namespaces-called))
            (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
      (testing "calls update-counter with correct args - w/o additional-tags argument"
        (let [metric-namespaces  [expected-topic-entity-name "metric-ns"]
              expected-namespace (str expected-topic-entity-name ".metric-ns")
              expected-tags      default-tags]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/increment-count metric-namespaces passed-metric-name))))
      (testing "calls update-counter with correct args - additional-tags is nil"
        (let [metric-namespace         "metric-ns"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              metric-namespaces-called (atom [])
              expected-tags            default-tags]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                            (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                      (is (= tags expected-tags))
                                                      (is (= metric passed-metric-name))
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
            (is (true? @increment-count-called?))))))
    (mount/stop #'metrics/statsd-reporter)))

(deftest decrement-count-test
  (with-redefs [config (assoc-in config [:ziggurat :metrics :constructor] "ziggurat.util.mock-metrics-implementation/->MockImpl")]
    (mount/start (mount/only [#'metrics/statsd-reporter]))
    (let [expected-topic-name "expected-topic-name"
          passed-metric-name  "metric3"
          input-tags          {:topic_name expected-topic-name}
          expected-n          -1
          n                   1]
      (testing "calls metrics library update-counter with the correct args - vector as an argument"
        (let [expected-tags              default-tags
              expected-metric-namespaces [expected-topic-name "metric-ns"]
              expected-namespace         (str expected-topic-name ".metric-ns")]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/decrement-count expected-metric-namespaces passed-metric-name n input-tags))))
      (testing "calls metrics library update-counter with the correct args - string as an argument"
        (let [expected-tags            (merge default-tags input-tags)
              metric-namespace         "metric-ns"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              metric-namespaces-called (atom [])]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                            (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                      (is (= tags expected-tags))
                                                      (is (= metric passed-metric-name))
                                                      (is (= value expected-n)))]
            (metrics/decrement-count metric-namespace passed-metric-name n input-tags)
            (is (some #{metric-namespace} @metric-namespaces-called))
            (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
      (testing "calls metrics library update-counter with the correct args - without topic name on the namespace"
        (let [expected-additional-tags (merge default-tags input-tags)
              metric-namespaces        ["metric" "ns"]
              expected-namespace       "metric.ns"
              expected-tags            (merge default-tags input-tags)]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (is (= namespace expected-namespace))
                                                      (is (= metric passed-metric-name))
                                                      (is (= tags expected-tags))
                                                      (is (= value expected-n)))]
            (metrics/decrement-count metric-namespaces passed-metric-name n input-tags))))
      (testing "calls metrics library update-counter with the correct args - additional-tags is nil"
        (let [expected-tags            default-tags
              metric-namespace         "metric-ns"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              metric-namespaces-called (atom [])]
          (with-redefs [mock-metrics/update-counter (fn [namespace metric tags value]
                                                      (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                            (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                      (is (= tags expected-tags))
                                                      (is (= metric passed-metric-name))
                                                      (is (= value expected-n)))]
            (metrics/decrement-count metric-namespace passed-metric-name n nil)
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
        (let [tags                    (doto (java.util.HashMap.)
                                        (.put ":foo" "bar")
                                        (.put ":bar" "foo"))
              expected-tags           {:foo "bar" :bar "foo"}
              decrement-count-called? (atom false)
              metric-namespace        "namespace"]
          (with-redefs [metrics/decrement-count (fn [actual-namespace actual-metric actual-additional-tags]
                                                  (if (and (= actual-namespace metric-namespace)
                                                           (= actual-metric passed-metric-name)
                                                           (= actual-additional-tags expected-tags))
                                                    (reset! decrement-count-called? true)))]
            (metrics/-decrementCount metric-namespace passed-metric-name tags)
            (is (true? @decrement-count-called?))))))
    (mount/stop #'metrics/statsd-reporter)))

(deftest report-histogram-test
  (with-redefs [config (assoc-in config [:ziggurat :metrics :constructor] "ziggurat.util.mock-metrics-implementation/->MockImpl")]
    (mount/start (mount/only [#'metrics/statsd-reporter]))
    (let [topic-entity-name "expected-topic-entity-name"
          input-tags        {:topic_name topic-entity-name}
          time-val          10
          expected-metric   "all"]
      (testing "calls update-histogram with the correct arguments - vector as an argument"
        (let [metric-namespaces         [topic-entity-name "message-received-delay-histogram"]
              expected-metric-namespace (str topic-entity-name ".message-received-delay-histogram")
              expected-tags             default-tags]
          (with-redefs [mock-metrics/update-timing (fn [namespace metric tags value]
                                                     (is (= namespace expected-metric-namespace))
                                                     (is (= metric expected-metric))
                                                     (is (= tags expected-tags))
                                                     (is (= value time-val)))]
            (metrics/report-histogram metric-namespaces time-val input-tags))))
      (testing "calls update-histogram with the correct arguments - string as an argument"
        (let [metric-namespace         "message-received-delay-histogram"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              expected-tags            (merge default-tags input-tags)
              metric-namespaces-called (atom [])]
          (with-redefs [mock-metrics/update-timing (fn [namespace metric tags value]
                                                     (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                           (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                     (is (= metric expected-metric))
                                                     (is (= tags expected-tags))
                                                     (is (= value time-val)))]
            (metrics/report-histogram metric-namespace time-val input-tags)
            (is (some #{metric-namespace} @metric-namespaces-called))
            (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
      (testing "calls update-histogram with the correct arguments - w/o additional-tags argument"
        (let [metric-namespace         "message-received-delay-histogram"
              actor-prefixed-metric-ns (str (:app-name (ziggurat-config)) "." metric-namespace)
              expected-tags            default-tags
              metric-namespaces-called (atom [])]
          (with-redefs [mock-metrics/update-timing (fn [namespace metric tags value]
                                                     (cond (= namespace metric-namespace) (swap! metric-namespaces-called conj namespace)
                                                           (= namespace actor-prefixed-metric-ns) (swap! metric-namespaces-called conj namespace))
                                                     (is (= metric expected-metric))
                                                     (is (= tags expected-tags))
                                                     (is (= value time-val)))]
            (metrics/report-histogram metric-namespace time-val)
            (is (some #{metric-namespace} @metric-namespaces-called))
            (is (some #{actor-prefixed-metric-ns} @metric-namespaces-called)))))
      (testing "report time java function passes the correct parameters to report time"
        (let [metric-namespace         "namespace"
              report-histogram-called? (atom false)]
          (with-redefs [metrics/report-histogram (fn [actual-metric-namespace actual-time-val]
                                                   (if (and (= actual-metric-namespace metric-namespace)
                                                            (= actual-time-val time-val))
                                                     (reset! report-histogram-called? true)))]
            (metrics/-reportTime metric-namespace time-val)
            (is (true? @report-histogram-called?))))
        (let [metric-namespace         "namespace"
              tags                     (doto (java.util.HashMap.)
                                         (.put ":foo" "bar")
                                         (.put ":bar" "foo"))
              expected-tags            {:foo "bar" :bar "foo"}
              report-histogram-called? (atom false)]
          (with-redefs [metrics/report-histogram (fn [actual-metric-namespace actual-time-val actual-additional-tags]
                                                   (is (= actual-additional-tags expected-tags))
                                                   (if (and (= actual-metric-namespace metric-namespace)
                                                            (= actual-time-val time-val)
                                                            (= actual-additional-tags expected-tags))
                                                     (reset! report-histogram-called? true)))]
            (metrics/-reportTime metric-namespace time-val tags)
            (is (true? @report-histogram-called?))))))
    (mount/stop #'metrics/statsd-reporter)))

(deftest report-time-test
  (let [metric-namespace "metric-namespace"
        value            12
        tags             {:foo "bar"}]
    (testing "report time passes the correct metric-namespace and values to report-histogram and logs deprecation notice"
      (with-redefs [metrics/report-histogram (fn [received-metric-ns received-val]
                                               (is (= metric-namespace received-metric-ns))
                                               (is (= value received-val)))]
        (metrics/report-time metric-namespace value)))
    (testing "report time passes the correct namespace, val and addition-tags to report-histogram and logs deprecation notice"
      (with-redefs [metrics/report-histogram (fn [received-metric-ns received-val received-tags]
                                               (is (= metric-namespace received-metric-ns))
                                               (is (= value received-val))
                                               (is (= tags received-tags)))]
        (metrics/report-time metric-namespace value tags)))))

(deftest multi-ns-report-time-test
  (testing "calls multi-ns-report-histogram with the correct arguments"
    (let [namespaces [["multi" "ns" "test"] ["multi-ns"]]
          time-val   123
          tags       {:foo "bar"}]
      (with-redefs [metrics/multi-ns-report-histogram (fn [received-nss received-time-val received-tags]
                                                        (is (= received-nss namespaces))
                                                        (is (= received-time-val time-val))
                                                        (is (= received-tags tags)))]
        (metrics/multi-ns-report-time namespaces time-val tags)))))

(deftest multi-ns-increment-count-test
  (testing "multi-ns-increment-count calls increment-count for every namespace list passed"
    (let [metric-namespaces-list               [["test" "multi" "ns"] ["test-ns"]]
          expected-metric                      "test-metric"
          expected-tags                        (merge default-tags {:foo "bar"})
          increment-count-call-counts          (atom 0)
          expected-increment-count-call-counts 2]
      (with-redefs [metrics/increment-count (fn [metric-namespaces metric tags]
                                              (when (and (some #{metric-namespaces} metric-namespaces-list)
                                                         (= metric expected-metric)
                                                         (= tags expected-tags))
                                                (swap! increment-count-call-counts inc)))]
        (metrics/multi-ns-increment-count metric-namespaces-list expected-metric expected-tags)
        (is (= expected-increment-count-call-counts @increment-count-call-counts))))))

(deftest multi-ns-report-histogram-test
  (testing "multi-ns-report-histogram calls report-histogram for every namespace list passed"
    (let [metric-namespaces-list                [["test" "multi" "ns"] ["test-ns"]]
          expected-metric                       "test-metric"
          expected-tags                         (merge default-tags {:foo "bar"})
          report-histogram-call-counts          (atom 0)
          expected-report-histogram-call-counts 2]
      (with-redefs [metrics/report-histogram (fn [metric-namespaces metric tags]
                                               (when (and (some #{metric-namespaces} metric-namespaces-list)
                                                          (= metric expected-metric)
                                                          (= tags expected-tags))
                                                 (swap! report-histogram-call-counts inc)))]
        (metrics/multi-ns-report-histogram metric-namespaces-list expected-metric expected-tags)
        (is (= expected-report-histogram-call-counts @report-histogram-call-counts))))))

(deftest initialise-metrics-library-test
  (testing "It sets the constructor object to default (dropwizard) when no value is passed in configuration"
    (with-redefs [ziggurat-config (constantly {:metrics {:constructor nil}})]
      (metrics/initialise-metrics-library)
      (is (instance? DropwizardMetrics (deref metrics/metric-impl)))))
  (testing "It sets the constructor object to the configured constructor's return value"
    (with-redefs [ziggurat-config (constantly {:metrics {:constructor "ziggurat.clj-statsd-metrics-wrapper/->CljStatsd"}})]
      (metrics/initialise-metrics-library)
      (is (instance? CljStatsd (deref metrics/metric-impl)))))
  (testing "It raises an exception when incorrect constructor has been configured"
    (with-redefs [ziggurat-config (constantly {:metrics {:constructor "incorrect-constructor"}})]
      (is (thrown? RuntimeException (metrics/initialise-metrics-library))))))