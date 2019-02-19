(ns ziggurat.kafka-delay-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-delay :refer :all]
            [ziggurat.metrics :as metrics])
  (:import [org.apache.kafka.streams.processor ProcessorContext]
           [org.apache.kafka.clients.consumer ConsumerRecord]
           [ziggurat.kafka_delay IngestionTimeExtractor]))

(deftest ingestion-time-extractor-test
  (let [ingestion-time-extractor (IngestionTimeExtractor.)
        topic "some-topic"
        partition (int 1)
        offset 1
        previous-timestamp 1528720768771
        key "some-key"
        value "some-value"
        record (ConsumerRecord. topic partition offset key value)]
    (testing "extract timestamp of topic when it has valid timestamp"
      (with-redefs [get-timestamp-from-record (constantly 1528720768777)]
        (is (= (.extract ingestion-time-extractor record previous-timestamp)
               1528720768777))))
    (testing "extract timestamp of topic when it has invalid timestamp"
      (with-redefs [get-timestamp-from-record (constantly -1)
                    get-current-time-in-millis (constantly 1528720768777)]
        (is (= (.extract ingestion-time-extractor record previous-timestamp)
               (get-current-time-in-millis)))))))

(deftest calculate-and-report-kafka-delay-test
  (testing "calculates and reports the timestamp delay"
    (let [record-timestamp 1528720767777
          current-time     1528720768777
          expected-delay   1000
          namespace        "test"]
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time        (fn [metric-namespace delay]
                                                 (is (= delay expected-delay))
                                                 (is (= metric-namespace namespace)))]
        (calculate-and-report-kafka-delay namespace record-timestamp)))))

(deftest timestamp-transformer-test
  (testing "creates a timestamp-transformer object that calculates and reports timestamp delay"
    (let [metric-namespace      "test.message-received-delay-histogram"
          record-timestamp      1528720767777
          context               (reify ProcessorContext
                                  (timestamp [_] record-timestamp))
          current-time          1528720768777
          timestamp-transformer (create-transformer metric-namespace current-time)
          expected-delay        1000]
      (.init timestamp-transformer context)
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time        (fn [namespace delay]
                                                 (is (= delay expected-delay))
                                                 (is (= metric-namespace namespace)))]
        (.transform timestamp-transformer nil nil)))))
