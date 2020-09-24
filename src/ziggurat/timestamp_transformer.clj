(ns ziggurat.timestamp-transformer
  (:require [ziggurat.kafka-delay :refer [calculate-and-report-kafka-delay]]
            [clojure.tools.logging :as log]
            [ziggurat.util.time :refer [get-current-time-in-millis get-timestamp-from-record]])
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

(deftype TimestampTransformer [^{:volatile-mutable true} processor-context metric-namespace oldest-processed-message-in-s additional-tags] Transformer
         (^void init [_ ^ProcessorContext context]
           (set! processor-context context))
         (transform [_ record-key record-value]
           (log/info "stream record metadata--> " "record-key: " record-key " record-value: " record-value " partition: " (.partition processor-context) " topic: " (.topic processor-context))
           (let [message-time (.timestamp processor-context)]
             (when (message-to-process? message-time oldest-processed-message-in-s)
               (calculate-and-report-kafka-delay metric-namespace message-time additional-tags)
               (KeyValue/pair record-key record-value))))
         (close [_] nil))

(defn create
  ([metric-namespace process-message-since-in-s]
   (create metric-namespace process-message-since-in-s nil))
  ([metric-namespace process-message-since-in-s additional-tags]
   (TimestampTransformer. nil metric-namespace process-message-since-in-s additional-tags)))
