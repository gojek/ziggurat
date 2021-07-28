(ns ziggurat.middleware.json-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [ziggurat.fixtures :as fix]
            [ziggurat.metrics :as metrics]
            [ziggurat.middleware.json :refer [parse-json]]))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(deftest parse-json-test
  (testing "Given a handler function (without passing key-fn), parse-json should call that function on after deserializing the string to JSON object."
    (let [handler-fn-called? (atom false)
          json-message       {:a "A"
                              :b "B"}
          topic-entity-name  "test"
          handler-fn         (fn [{:keys [message metadata]}]
                               (when (and (= json-message message)
                                          (= (:topic metadata) "topic")
                                          (= (:timestamp metadata) 1234567890)
                                          (= (:partition metadata) 1))
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name) {:message (generate-string json-message) :metadata {:topic "topic" :timestamp 1234567890 :partition 1}})
      (is (true? @handler-fn-called?))))
  (testing "Given a handler function and key-fn as false, parse-json should call that function on after
            deserializing the string without coercing the keys to keywords."
    (let [handler-fn-called? (atom false)
          json-message       {"a" "A" "b" "B"}
          topic-entity-name  "test"
          handler-fn         (fn [{:keys [message metadata]}]
                               (when (and (= json-message message)
                                          (= (:topic metadata) "topic")
                                          (= (:timestamp metadata) 1234567890)
                                          (= (:partition metadata) 1))
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name false) {:message (generate-string json-message) :metadata {:topic "topic" :timestamp 1234567890 :partition 1}})
      (is (true? @handler-fn-called?))))
  (testing "Given a handler function and a key-fn, parse-json should call that function after
            deserializing the string by applying key-fn to keys."
    (let [handler-fn-called? (atom false)
          key-fn             (fn [k] (str k "-modified"))
          json-message       {"a" "A"
                              "b" "B"}
          topic-entity-name  "test"
          handler-fn         (fn [{:keys [message metadata]}]
                               (is (= {"a-modified" "A", "b-modified" "B"} message))
                               (when (and (= {"a-modified" "A", "b-modified" "B"} message)
                                          (= (:topic metadata) "topic")
                                          (= (:timestamp metadata) 1234567890)
                                          (= (:partition metadata) 1))
                                 (reset! handler-fn-called? true)))]
      ((parse-json handler-fn topic-entity-name key-fn) {:message (generate-string json-message) :metadata {:topic "topic" :timestamp 1234567890 :partition 1}})
      (is (true? @handler-fn-called?))))
  (testing "Should report metrics when JSON deserialization fails"
    (let [handler-fn-called?      (atom false)
          metric-reporter-called? (atom false)
          topic-entity-name       "test"
          json-message            "{\"foo\":\"bar"
          handler-fn              (fn [{:keys [message _]}]
                                    (when (nil? message)
                                      (reset! handler-fn-called? true)))]
      (with-redefs [metrics/multi-ns-increment-count (fn [_ _ _]
                                                       (reset! metric-reporter-called? true))]
        ((parse-json handler-fn topic-entity-name true) {:message json-message :metadata {:topic "topic" :timestamp 1234567890 :partition 1}}))
      (is (true? @handler-fn-called?))
      (is (true? @metric-reporter-called?)))))
