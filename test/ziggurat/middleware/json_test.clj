(ns ziggurat.middleware.json-test
  (:require [clojure.test :refer :all]
            [cheshire.core :refer [generate-string]]
            [ziggurat.middleware.json :refer [parse-json]]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest parse-json-test
  (testing "Given a handler function (without passing key-fn), parse-json should call that function on after deserializing the string to JSON object."
    (let [handler-fn-called? (atom false)
          message            {:a "A"
                              :b "B"}
          topic-entity-name  "test"
          handler-fn         (fn [msg]
                               (if (= msg message)
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name) (generate-string message))
      (is (true? @handler-fn-called?))))
  (testing "Given a handler function and key-fn as false, parse-json should call that function on after
            deserializing the string without coercing the keys to keywords."
    (let [handler-fn-called? (atom false)
          message            {:a "A"
                              :b "B"}
          expected-output    {"a" "A" "b" "B"}
          topic-entity-name  "test"
          handler-fn         (fn [msg]
                               (if (= msg expected-output)
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name false) (generate-string message))
      (is (true? @handler-fn-called?))))
  (testing "Given a handler function and a key-fn, parse-json should call that function after
            deserializing the string by applying key-fn to keys."
    (let [handler-fn-called? (atom false)
          key-fn             (fn [k] (str k "-modified"))
          message            {"a" "A"
                              "b" "B"}
          expected-output    {"a-modified" "A" "b-modified" "B"}
          topic-entity-name  "test"
          handler-fn         (fn [msg]
                               (if (= msg expected-output)
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name key-fn) (generate-string message))
      (is (true? @handler-fn-called?))))
  (testing "Should report metrics when JSON deserialization fails"
    (let [handler-fn-called?      (atom false)
          metric-reporter-called? (atom false)
          topic-entity-name       "test"
          message                 "{\"foo\":\"bar"
          handler-fn              (fn [msg]
                                    (if (nil? msg)
                                      (reset! handler-fn-called? true)))]
      (with-redefs [metrics/multi-ns-increment-count (fn [_ _ _]
                                              (reset! metric-reporter-called? true))]
        ((parse-json handler-fn topic-entity-name true) message))
      (is (true? @handler-fn-called?))
      (is (true? @metric-reporter-called?)))))
