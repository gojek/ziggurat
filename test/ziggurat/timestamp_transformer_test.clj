(ns ziggurat.timestamp-transformer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ziggurat.metrics :as metrics]
            [ziggurat.timestamp-transformer :refer [create]]
            [ziggurat.util.time :refer [get-current-time-in-millis get-timestamp-from-record]])
  (:import [org.apache.kafka.clients.consumer ConsumerRecord]
           [org.apache.kafka.streams.processor ProcessorContext]
           [ziggurat.timestamp_transformer IngestionTimeExtractor]
           [org.apache.kafka.common.header.internals RecordHeaders]))

(deftest ingestion-time-extractor-test
  (let [ingestion-time-extractor (IngestionTimeExtractor.)
        topic                    "some-topic"
        partition                (int 1)
        offset                   1
        previous-timestamp       1528720768771
        key                      "some-key"
        value                    "some-value"
        record                   (ConsumerRecord. topic partition offset key value)]
    (testing "extract timestamp of topic when it has valid timestamp"
      (with-redefs [get-timestamp-from-record (constantly 1528720768777)]
        (is (= (.extract ingestion-time-extractor record previous-timestamp)
               1528720768777))))
    (testing "extract timestamp of topic when it has invalid timestamp"
      (with-redefs [get-timestamp-from-record  (constantly -1)
                    get-current-time-in-millis (constantly 1528720768777)]
        (is (= (.extract ingestion-time-extractor record previous-timestamp)
               (get-current-time-in-millis)))))))

(deftest timestamp-transformer-test
  (let [default-namespace          "message-received-delay-histogram"
        expected-metric-namespaces ["test" default-namespace]
        record-timestamp           1528720767777
        current-time               1528720768777
        expected-delay             1000
        test-partition             1337
        test-topic                 "test-topic"
        my-key                     "my-key"
        my-value                   "my-value"]
    (testing "creates a timestamp-transformer object that calculates and reports timestamp delay"
      (let [context                    (reify ProcessorContext
                                         (timestamp [_] record-timestamp)
                                         (partition [_] test-partition)
                                         (topic [_] test-topic))
            expected-topic-entity-name "expected-topic-entity-name"
            timestamp-transformer      (create expected-metric-namespaces current-time expected-topic-entity-name)]
        (.init timestamp-transformer context)
        (with-redefs [get-current-time-in-millis (constantly current-time)
                      metrics/report-histogram        (fn [metric-namespaces delay topic-entity-name]
                                                        (is (= delay expected-delay))
                                                        (is (or (= metric-namespaces expected-metric-namespaces)
                                                                (= metric-namespaces [default-namespace])))
                                                        (is (= expected-topic-entity-name topic-entity-name)))]
          (.transform timestamp-transformer my-key my-value))))
    (testing "creates a timestamp-transformer object that calculates and reports timestamp delay when topic-entity-name is nil"
      (let [context                    (reify ProcessorContext
                                         (timestamp [_] record-timestamp)
                                         (partition [_] test-partition)
                                         (topic [_] test-topic))
            expected-topic-entity-name nil
            timestamp-transformer      (create expected-metric-namespaces current-time)]
        (.init timestamp-transformer context)
        (with-redefs [get-current-time-in-millis (constantly current-time)
                      metrics/report-histogram        (fn [metric-namespaces delay topic-entity-name]
                                                        (is (= delay expected-delay))
                                                        (is (or (= metric-namespaces expected-metric-namespaces)
                                                                (= metric-namespaces [default-namespace])))
                                                        (is (= topic-entity-name expected-topic-entity-name)))]
          (.transform timestamp-transformer my-key my-value))))))
