(ns ziggurat.timestamp-transformer-test
  (:require [clojure.test :refer :all]
            [ziggurat.metrics :as metrics]
            [ziggurat.timestamp-transformer :refer :all]
            [ziggurat.util.time :refer :all])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.streams.processor ProcessorContext]
           [ziggurat.timestamp_transformer IngestionTimeExtractor]))

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

(deftest timestamp-transformer-test
  (testing "creates a timestamp-transformer object that calculates and reports timestamp delay"
    (let [metric-namespace           "test.message-received-delay-histogram"
          record-timestamp           1528720767777
          context                    (reify ProcessorContext
                                       (timestamp [_] record-timestamp))
          current-time               1528720768777
          expected-topic-entity-name "expected-topic-entity-name"
          timestamp-transformer      (create metric-namespace current-time expected-topic-entity-name)
          expected-delay             1000]
      (.init timestamp-transformer context)
      (with-redefs [get-current-time-in-millis (constantly current-time)
                    metrics/report-time        (fn [namespace delay topic-entity-name]
                                                 (is (= delay expected-delay))
                                                 (is (= metric-namespace namespace))
                                                 (is (= expected-topic-entity-name topic-entity-name)))]
        (.transform timestamp-transformer nil nil)))))
