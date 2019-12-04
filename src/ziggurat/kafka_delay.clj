(ns ziggurat.kafka-delay
  (:require [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer :all]))

(defn calculate-and-report-kafka-delay
  ([metric-namespaces record-timestamp]
   (calculate-and-report-kafka-delay metric-namespaces record-timestamp nil))
  ([metric-namespaces record-timestamp additional-tags]
   (let [now-millis        (get-current-time-in-millis)
         delay             (- now-millis record-timestamp)
         default-namespace (last metric-namespaces)
         multi-namespaces  [metric-namespaces [default-namespace]]]
     (metrics/multi-ns-report-histogram multi-namespaces delay additional-tags))))
