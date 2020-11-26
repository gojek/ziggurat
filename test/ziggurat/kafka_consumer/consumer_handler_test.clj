(ns ziggurat.kafka-consumer.consumer-handler-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.message-payload :as mp]
            [ziggurat.config :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.metrics :as metrics])
  (:import (org.apache.kafka.clients.consumer Consumer ConsumerRecords ConsumerRecord)
           (org.apache.kafka.common.errors WakeupException)
           (java.time Duration)
           (java.util HashMap ArrayList)
           (org.apache.kafka.common TopicPartition)))

(use-fixtures :once fix/mount-only-config)

(def dummy-consumer-records
  (let [topic-partition (TopicPartition. "string" 1)
        individual-consumer-record (ConsumerRecord. "topic" 1 2 "hello" "world")
        list-of-consumer-records (doto (ArrayList.) (.add individual-consumer-record))
        map-of-partition-and-records (doto (HashMap.) (.put topic-partition list-of-consumer-records))]
    (ConsumerRecords. map-of-partition-and-records)))

(deftest consumer-polling-test
  (testing "should commit only if non-zero records are polled"
    (let [expected-calls 2
          actual-calls   (atom 0)
          commit-called  (atom false)
          kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration _]
                             dummy-consumer-records)
                           (commitSync [_] (reset! commit-called true))
                           (close [_]))]
      (with-redefs [ch/process (fn [_ _]
                                 (if (< @actual-calls 2)
                                   (swap! actual-calls inc)
                                   (throw (WakeupException.))))
                    metrics/increment-count (constantly nil)]
        (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config 1000})
        (is (= expected-calls @actual-calls))
        (is (true? @commit-called)))))
  (testing "should not commit if no records are polled"
    (let [expected-calls 0
          actual-calls   (atom 0)
          commit-called  (atom false)
          kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration _]
                             [])
                           (commitSync [_] (reset! commit-called true))
                           (close [_]))]
      (with-redefs [ch/process (fn [_ _]
                                 (if (< @actual-calls 2)
                                   (swap! actual-calls inc)
                                   (throw (WakeupException.))))
                    metrics/increment-count (constantly nil)]
        (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config 1000})
        (is (= expected-calls @actual-calls))
        (is (false? @commit-called)))))
  (testing "should keep on polling even if commitSync call on KafkaConsumer throws an exception and publishes the metrics"
    (let [expected-calls 2
          actual-calls (atom 0)
          kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration _]
                             dummy-consumer-records)
                           (commitSync [_]
                             (throw (Exception. "Commit exception")))
                           (close [_]))]
      (with-redefs [ch/process (fn [_ _]
                                 (if (< @actual-calls 2)
                                   (swap! actual-calls inc)
                                   (throw (WakeupException.))))
                    metrics/increment-count (fn [metric-namespace metrics _ tags]
                                              (is (= metric-namespace ["ziggurat.batch.consumption" "message.processed"]))
                                              (is (= metrics "commit.failed.exception"))
                                              (is (= "random-consumer-id" (:topic-entity tags))))]
        (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config 1000})
        (is (= expected-calls @actual-calls)))))
  (testing "Exceptions other than WakeupException are caught"
    (let [kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration _]
                             dummy-consumer-records)
                           (commitSync [_])
                           (close [_]))]
      (with-redefs [ch/process (fn [_ _] (throw (Exception.)))
                    metrics/increment-count (constantly nil)]
        (is (= nil (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config 1000}))))))
  (testing "Uses DEFAULT_POLL_TIMEOUT_MS_CONFIG if :poll-timeout-ms-config is not configured"
    (let [topic-partition (TopicPartition. "string" 1)
          individual-consumer-record (ConsumerRecord. "topic" 1 2 "hello" "world")
          list-of-consumer-records (doto (ArrayList.) (.add individual-consumer-record))
          map-of-partition-and-records (doto (HashMap.) (.put topic-partition list-of-consumer-records))
          records (ConsumerRecords. map-of-partition-and-records)
          actual-poll-timeout  (atom 0)
          process-calls        (atom 0)
          kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration timeout]
                             (reset! actual-poll-timeout (.toMillis timeout))
                             records)
                           (commitSync [_])
                           (close [_]))]
      (with-redefs [ch/process (fn [_ _]
                                 (if (< @process-calls 1)
                                   (swap! process-calls inc)
                                   (throw (Exception.))))
                    metrics/increment-count (constantly nil)]
        (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config nil})
        (is (= ch/DEFAULT_POLL_TIMEOUT_MS_CONFIG @actual-poll-timeout)))))
  (testing "create message payload from values of consumer-record and pass it to the process function"
    (let [topic-partition (TopicPartition. "string" 1)
          individual-consumer-record (ConsumerRecord. "topic" 1 2 "hello" "world")
          list-of-consumer-records (doto (ArrayList.) (.add individual-consumer-record))
          map-of-partition-and-records (doto (HashMap.) (.put topic-partition list-of-consumer-records))
          records (ConsumerRecords. map-of-partition-and-records)
          is-polled (atom 0)
          kafka-consumer (reify Consumer
                           (^ConsumerRecords poll [_ ^Duration _]
                             (if (< @is-polled 1)
                               (do
                                 (swap! is-polled inc)
                                 records)
                               (throw (WakeupException.))))
                           (commitSync [_])
                           (close [_]))]
      (with-redefs [ch/process (fn [batch-handler message]
                                 (when-not (empty? (:batch message))
                                   (is (= (:value (first (:batch message))) "world"))
                                   (is (= (:topic-entity message) :random-consumer-id))
                                   (is (= (:retry-count message) nil))))]
        (ch/poll-for-messages kafka-consumer nil :random-consumer-id {:consumer-group-id "some-id" :poll-timeout-ms-config 1000})))))

(deftest process-function-test
  (testing "should publish metrics for batch size, success count, failure count, retry-count and execution time after processing is finished"
    (let [expected-batch-size    10
          expected-success-count  6
          expected-skip-count     2
          expected-retry-count    2
          batch-handler          (fn [_] {:skip (vec (replicate expected-skip-count 0)) :retry (vec (replicate expected-retry-count 0))})]
      (with-redefs [metrics/increment-count (fn [_ metrics count tags]
                                              (is (= "consumer-1" (:topic-entity tags)))
                                              (cond
                                                (= metrics "total")
                                                (is (= expected-batch-size count))
                                                (= metrics "success")
                                                (is (= expected-success-count count))
                                                (= metrics "skip")
                                                (is (= expected-skip-count count))
                                                (= metrics "retry")
                                                (is (= expected-retry-count count))))
                    metrics/report-time     (fn [metric-namespace _ _]
                                              (is (= metric-namespace ["ziggurat.batch.consumption" "message.processed" "execution-time"])))
                    ch/retry (fn [batch current-retry-count topic-entity]
                               (is (= batch (vec (replicate expected-retry-count 0))))
                               (is (= current-retry-count nil))
                               (is (= topic-entity :consumer-1)))]
        (ch/process batch-handler (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil})))))
  (testing "should publish metrics for exception in process message"
    (let [expected-batch-size    10
          batch-handler          (fn [_] (throw (Exception. "exception in batch-handler")))]
      (with-redefs [metrics/increment-count (fn [metric-namespace metrics count tags]
                                              (is (= count expected-batch-size))
                                              (is (= metric-namespace ["ziggurat.batch.consumption" "message.processed"]))
                                              (is (= metrics "exception"))
                                              (is (= "consumer-1" (:topic-entity tags))))
                    ch/retry (fn [message]
                               (is (= message (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil}))))]
        (ch/process batch-handler (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil})))))
  (testing "should process the batch only when its non-empty"
    (let [batch-size     10
          processed      (atom false)
          batch-handler  (fn [_] (reset! processed true))]
      (ch/process batch-handler (mp/map->MessagePayload {:message (vec (replicate batch-size 0)) :topic-entity :consumer-1 :retry-count nil}))
      (is (true? @processed))))
  (testing "should NOT process the batch if its empty"
    (let [processed      (atom false)
          batch-handler  (fn [_] (reset! processed true))]
      (ch/process batch-handler (mp/map->MessagePayload {:message [] :topic-entity :consumer-1 :retry-count nil}))
      (is (false? @processed)))))

(deftest retry-test
  (testing "when batch handler returns non-empty retry vector those message should be added to rabbitmq retry queue"
    (let [expected-batch-size    10
          expected-retry-count    3
          retry-messages          (vec (replicate expected-retry-count 0))
          batch-handler          (fn [_] {:skip [] :retry (vec (replicate expected-retry-count 0))})
          batch-payload        (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil})
          retried                (atom false)]
      (with-redefs [producer/retry (fn [message]
                                     (reset! retried true)
                                     (is (= message (mp/map->MessagePayload {:message retry-messages :topic-entity :consumer-1 :retry-count nil}))))
                    metrics/increment-count (constantly nil)
                    metrics/report-time (constantly nil)]
        (ch/process batch-handler batch-payload)
        (is (= true @retried)))))
  (testing "when batch handler returns empty retry vector those message should not be added to rabbitmq retry queue"
    (let [expected-batch-size    10
          expected-retry-count    0
          batch-handler          (fn [_] {:skip [] :retry (vec (replicate expected-retry-count 0))})
          retried                (atom false)
          batch-payload        (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil})]
      (with-redefs [producer/retry (fn [message]
                                     (reset! retried true))
                    metrics/increment-count (constantly nil)
                    metrics/report-time (constantly nil)]
        (ch/process batch-handler batch-payload)
        (is (= false @retried)))))
  (testing "when batch handler throws exception all messages should be added to rabbitmq retry queue"
    (let [expected-batch-size    10
          batch-handler          (fn [_] (throw (Exception. "batch handler exception")))
          batch-payload        (mp/map->MessagePayload {:message (vec (replicate expected-batch-size 0)) :topic-entity :consumer-1 :retry-count nil})
          retried                (atom false)]
      (with-redefs [producer/retry (fn [message]
                                     (reset! retried true)
                                     (is (= message batch-payload)))
                    metrics/increment-count (constantly nil)
                    metrics/report-time (constantly nil)]
        (ch/process batch-handler batch-payload)
        (is (= true @retried))))))
