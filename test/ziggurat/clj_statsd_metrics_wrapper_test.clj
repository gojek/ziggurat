(ns ziggurat.clj-statsd-metrics-wrapper-test
  (:require [clojure.test :refer :all]
            [ziggurat.clj-statsd-metrics-wrapper :refer :all :as clj-statsd-wrapper]
            [clj-statsd :as statsd]))

(def passed-host "localhost")
(def passed-port 8125)
(def passed-enabled true)
(def statsd-config {:host passed-host :port passed-port :enabled passed-enabled})

(deftest initialise-test
  (testing "it calls the setup library function when enabled is true"
    (let [setup-called? (atom false)]
      (with-redefs [statsd/setup (fn [host port]
                                   (is (= host passed-host))
                                   (is (= port passed-port))
                                   (reset! setup-called? true))]
        (initialize statsd-config)
        (is (true? @setup-called?)))))
  (testing "it does not call setup library function when enabled is false"
    (let [setup-called? (atom false)
          statsd-config (assoc statsd-config :enabled false)]
      (with-redefs [statsd/setup (fn [_ _]
                                   (reset! setup-called? true))]
        (initialize statsd-config)
        (is (false? @setup-called?))))))

(deftest terminate-test
  (testing "it sets the cfg and sockagt states to nil in the library"
    (let [reset-called? (atom nil)]
      (initialize statsd-config)
      (is (some? statsd/cfg))
      (is (some? statsd/sockagt))
      (terminate)
      (is (nil? @statsd/cfg))
      (is (nil? (await statsd/sockagt))))))

(let [namespace        "namespace"
      metric           "metric"
      expected-metric  "namespace.metric"
      tags             {:key :val :foo "bar" :foobar 2}
      expected-tags    ["key:val" "foo:bar" "foobar:2"]]
  (deftest update-counter-test
    (testing "it calls statsd/increment with the correctly formatted arguments"
      (let [value            -1
            increment-called (atom false)]
        (with-redefs [statsd/increment (fn [k v rate tags]
                                         (is (= expected-metric k))
                                         (is (= value v))
                                         (is (= clj-statsd-wrapper/rate rate))
                                         (is (= expected-tags tags))
                                         (reset! increment-called true))]
          (update-counter namespace metric tags value)
          (is (true? @increment-called))))))

  (deftest update-timing-test
    (testing "it calls statsd/timing with the correctly formatted arguments"
      (let [value 100
            timing-called (atom false)]
        (with-redefs [statsd/timing (fn [k v rate tags]
                                      (is (= expected-metric k))
                                      (is (= value v))
                                      (is (= clj-statsd-wrapper/rate rate))
                                      (is (= expected-tags tags))
                                      (reset! timing-called true))]
          (update-timing namespace metric tags value)
          (is (true? @timing-called)))))))