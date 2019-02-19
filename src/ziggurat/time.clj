(ns ziggurat.time
  (:import [java.time Instant]))

(defn get-timestamp-from-record [record]
  (.timestamp record))

(defn get-current-time-in-millis []
  (.toEpochMilli (Instant/now)))

