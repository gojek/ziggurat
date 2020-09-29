(ns ziggurat.timestamp-transformer
  (:require [ziggurat.kafka-delay :refer [calculate-and-report-kafka-delay]]
            [clojure.tools.logging :as log]
            [ziggurat.util.time :refer [get-current-time-in-millis get-timestamp-from-record]])
  (:import [org.apache.kafka.streams KeyValue]
           [org.apache.kafka.streams.kstream Transformer]
           [org.apache.kafka.streams.processor TimestampExtractor ProcessorContext]))

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
           ;;(log/debug "stream record metadata--> " "record-key: " record-key " record-value: " record-value " partition: " (.partition processor-context) " topic: " (.topic processor-context))
           (let [message-time     (.timestamp processor-context)
                 partition        (.partition processor-context)
                 topic            (.topic processor-context)
                 new-record-value (with-meta 'record-value {:timestamp message-time
                                                            :topic topic
                                                            :additional-tags additional-tags
                                                            :metric-namespace metric-namespace
                                                            :partition partition})]

             (KeyValue/pair record-key new-record-value)))
         (close [_] nil))

(defn create
  ([metric-namespace process-message-since-in-s]
   (create metric-namespace process-message-since-in-s nil))
  ([metric-namespace process-message-since-in-s additional-tags]
   (TimestampTransformer. nil metric-namespace process-message-since-in-s additional-tags)))
