(ns ziggurat.kafka-delay
  (:require [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer :all]))

(defn calculate-and-report-kafka-delay
  ([metric-namespace record-timestamp]
   (calculate-and-report-kafka-delay metric-namespace record-timestamp nil))
  ([metric-namespace record-timestamp additional-tags]
   (let [now-millis (get-current-time-in-millis)
         delay      (- now-millis record-timestamp)]
     (metrics/report-time metric-namespace delay additional-tags))))
