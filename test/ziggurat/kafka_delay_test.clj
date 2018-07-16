(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [lambda-common.metrics :as metrics])
  (:import [org.apache.kafka.streams.processor ProcessorContext]))

(deftest calculate-and-report-kafka-delay-test
  (testing "calculates and reports the timestamp delay"
    (let [record-timestamp 1528720767777
          current-time 1528720768777
          expected-delay 1000
          namespace "test"]
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time (fn [metric-namespace delay]
                                          (is (= delay expected-delay))
                                          (is (= metric-namespace namespace)))]
        (calculate-and-report-kafka-delay namespace record-timestamp)))))

(deftest timestamp-transformer-test
  (testing "creates a timestamp-transformer object that calculates and reports timestamp delay"
    (let [metric-namespace "test.message-received-delay-histogram"
          record-timestamp 1528720767777
          timestamp-transformer (create-transformer metric-namespace)
          context (reify ProcessorContext
                    (timestamp [_] record-timestamp))
          current-time 1528720768777
          expected-delay 1000]
      (.init timestamp-transformer context)
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time (fn [namespace delay]
                                          (is (= delay expected-delay))
                                          (is (= metric-namespace namespace)))]
        (.transform timestamp-transformer nil nil))
      )))
