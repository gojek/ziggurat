(ns ziggurat.kafka-delay
  (:require [ziggurat.metrics :as metrics]
            [ziggurat.util.time :refer [get-current-time-in-millis]]))

(defn calculate-and-report-kafka-delay
  ([metric-namespaces record-timestamp]
   (calculate-and-report-kafka-delay metric-namespaces record-timestamp nil))
  ([metric-namespaces record-timestamp additional-tags]
   (let [now-millis        (get-current-time-in-millis)
         delay             (- now-millis record-timestamp)
         default-namespace (last metric-namespaces)
         multi-namespaces  [metric-namespaces [default-namespace]]]
     (metrics/prom-inc :ziggurat/kafka-delay-time additional-tags delay)
     (metrics/multi-ns-report-histogram multi-namespaces delay additional-tags))))
