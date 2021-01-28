(ns ziggurat.kafka-consumer.consumer-handler
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer :all]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.message-payload :refer [map->MessagePayload]]
            [ziggurat.metrics :as metrics])
  (:import (org.apache.kafka.common.errors WakeupException)
           (java.time Duration Instant)
           (tech.gojek.ziggurat.internal InvalidReturnTypeException)
           (org.apache.kafka.clients.consumer Consumer ConsumerRecord)))

(def DEFAULT_POLL_TIMEOUT_MS_CONFIG 1000)
(def batch-consumption-metric-ns ["ziggurat.batch.consumption" "message.processed"])

(defn- publish-batch-process-metrics
  [topic-entity batch-size success-count skip-count retry-count time-taken-in-millis]
  (let [topic-entity-tag {:topic-entity (name topic-entity)}]
    (metrics/increment-count batch-consumption-metric-ns "total" batch-size topic-entity-tag)
    (metrics/increment-count batch-consumption-metric-ns "success" success-count topic-entity-tag)
    (metrics/increment-count batch-consumption-metric-ns "skip" skip-count topic-entity-tag)
    (metrics/increment-count batch-consumption-metric-ns "retry" retry-count topic-entity-tag)
    (metrics/report-time (conj batch-consumption-metric-ns "execution-time") time-taken-in-millis topic-entity-tag)))

(defn- retry
  ([batch-payload]
   (producer/retry batch-payload))
  ([batch current-retry-count topic-entity]
   (when (pos? (count batch))
     (let [message (map->MessagePayload {:message         batch
                                         :retry-count        current-retry-count
                                         :topic-entity topic-entity})]
       (producer/retry message)))))

(defn validate-return-type
  [result]
  (and (map? result) (= (set (keys result)) #{:skip :retry})
       (vector? (:skip result)) (vector? (:retry result))))

(defn validate-batch-processing-result
  [result]
  (when-not (validate-return-type result)
    (throw (InvalidReturnTypeException. "Invalid result received from batch-handler. Please return a map with skip and retry vectors like {:skip [] :retry []}"))))

(defn process
  [batch-handler batch-payload]
  (let [batch               (:message batch-payload)
        topic-entity        (:topic-entity batch-payload)
        current-retry-count (:retry-count batch-payload)
        batch-size          (count batch)]
    (try
      (when (not-empty batch)
        (log/infof "[Consumer Group: %s] Processing the batch with %d messages" topic-entity batch-size)
        (let [start-time             (Instant/now)
              result                 (batch-handler batch)
              time-taken-in-millis   (.toMillis (Duration/between start-time (Instant/now)))]
          (validate-batch-processing-result result)
          (let [messages-to-be-retried (:retry result)
                to-be-retried-count    (count messages-to-be-retried)
                skip-count             (count (:skip result))
                success-count          (- batch-size (+ to-be-retried-count skip-count))]
            (log/infof "[Consumer Group: %s] Processed the batch with success: [%d], skip: [%d] and retries: [%d] \n"
                       topic-entity success-count skip-count to-be-retried-count)
            (publish-batch-process-metrics topic-entity batch-size success-count skip-count to-be-retried-count time-taken-in-millis)
            (retry messages-to-be-retried current-retry-count topic-entity))))
      (catch InvalidReturnTypeException e
        (throw e))
      (catch Exception e
        (do
          (metrics/increment-count batch-consumption-metric-ns "exception" batch-size {:topic-entity (name topic-entity)})
          (log/errorf e "[Consumer Group: %s] Exception received while processing messages \n" topic-entity)
          (retry batch-payload))))))

(defn- commit-offsets
  [consumer topic-entity]
  (try
    (.commitSync consumer)
    (catch Exception e
      (metrics/increment-count batch-consumption-metric-ns "commit.failed.exception" 1 {:topic-entity (name topic-entity)})
      (log/error "Exception while committing offsets:" e))))

(defn- create-batch-payload
  [records topic-entity]
  (let [key-value-pairs (map (fn [^ConsumerRecord m]
                               {:value (.value m) :key (.key m)}) records)]
    (map->MessagePayload {:message key-value-pairs :topic-entity topic-entity})))

(defn poll-for-messages
  [^Consumer consumer handler-fn topic-entity consumer-config]
  (try
    (loop [records []]
      (when (not-empty records)
        (let [batch-payload (create-batch-payload records topic-entity)]
          (process handler-fn batch-payload))
        (commit-offsets consumer topic-entity))
      (recur (seq (.poll consumer (Duration/ofMillis (or (:poll-timeout-ms-config consumer-config) DEFAULT_POLL_TIMEOUT_MS_CONFIG))))))
    (catch WakeupException e)
    (catch Exception e
      (log/errorf e "Exception while polling for messages for: %s" topic-entity))
    (finally (do (log/info "Closing the Kafka Consumer for: " topic-entity)
                 (.close consumer)))))

