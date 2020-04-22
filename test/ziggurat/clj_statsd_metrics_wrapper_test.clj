(ns ziggurat.clj-statsd-metrics-wrapper-test
  (:require [clojure.test :refer :all]
            [ziggurat.clj-statsd-metrics-wrapper :refer :all]
            [clj-statsd :as statsd]))

(def passed-host "localhost")
(def passed-port 8125)
(def passed-enabled true)
(def statsd-config {:host passed-host :port passed-port :enabled passed-enabled})

(deftest initialise-test
  (testing "it calls the setup library function when enabled is true"
    (let [setup-called?  (atom false)]
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