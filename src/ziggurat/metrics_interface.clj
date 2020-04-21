(ns ziggurat.metrics-interface)

(defprotocol MetricsLib
  (initialize [impl statsd-config])
  (terminate  [impl])
  (update-counter [impl namespace metric tags sign value])
  (update-histogram [impl namespace metric tags value]))


