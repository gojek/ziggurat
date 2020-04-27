(ns ziggurat.metrics-interface)

(defprotocol MetricsProtocol
  (initialize [impl statsd-config])
  (terminate  [impl])
  (update-counter [impl namespace metric tags signed-val])
  (update-timing [impl namespace metric tags value]))

