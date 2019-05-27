(ns ziggurat.timestamp-transformer
  (:require [ziggurat.kafka-delay :refer :all]
            [ziggurat.util.time :refer :all])
  (:import [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams.processor TimestampExtractor ProcessorContext]))

(defn- message-to-process? [message-timestamp oldest-processed-message-in-s]
  (let [current-time (get-current-time-in-millis)
        allowed-time (- current-time (* 1000 oldest-processed-message-in-s))]
    (> message-timestamp allowed-time)))

(deftype IngestionTimeExtractor [] TimestampExtractor
         (extract [_ record _]
           (let [ingestion-time (get-timestamp-from-record record)]
             (if (neg? ingestion-time)
               (get-current-time-in-millis)
               ingestion-time))))

(deftype TimestampTransformer [^{:volatile-mutable true} processor-context metric-namespace oldest-processed-message-in-s topic-entity-name] Transformer
         (^void init [_ ^ProcessorContext context]
           (do (set! processor-context context)
               nil))
         (transform [_ record-key record-value]
           (let [message-time (.timestamp processor-context)]
             (when (message-to-process? message-time oldest-processed-message-in-s)
               (calculate-and-report-kafka-delay metric-namespace message-time topic-entity-name)
               (KeyValue/pair record-key record-value))))
         (close [_] nil))

(defn create
  ([metric-namespace process-message-since-in-s]
   (create metric-namespace process-message-since-in-s nil))
  ([metric-namespace process-message-since-in-s topic-entity-name]
   (TimestampTransformer. nil metric-namespace process-message-since-in-s topic-entity-name)))
