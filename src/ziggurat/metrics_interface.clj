(ns ziggurat.metrics-interface)

(defprotocol MetricsLib
  (initialize [impl statsd-config])
  (terminate  [impl])
  (update-counter [impl namespace metric tags signed-int-val])
  (update-histogram [impl namespace metric tags value]))


