(ns ziggurat.prometheus.standalone
  (:require [clojure.tools.logging :as log]
            [iapetos.standalone :as standalone]
            [ziggurat.prometheus.core :refer [reg]]
            [iapetos.core :as prometheus]
            [ziggurat.config :refer [ziggurat-config]]
            [clojure.string :refer [starts-with? includes? trim]]
            [mount.core :as mount :refer [defstate]])
  (:import (ziggurat.metrics_interface MetricsProtocol)))

(defn- start [port]
  (log/info "Starting standalone server on port:" port)
  (standalone/metrics-server @reg {:port 8081}))

(defn- stop
  [server]
  (.close server)
  (log/info "Stopped standalone server"))

(defstate server
  :start (start (-> (ziggurat-config) :prometheus))
  :stop (stop server))

(defn initialize [prom-config]
  (-> (mount/only #{#'server})
      (mount/start)))

(defn terminate []
  (mount/stop  #{#'server}))

(defn update-prom-metric
  [namespace metric tags value]
  (let [common-labels {:topic-name (or (get tags :topic_name) (get tags :topic-entity))
                       :actor        (-> (ziggurat-config) :app-name)
                       :env        (get tags :env)}]
    (cond
    ;; needs to be more specific to less specific
      (and (includes? namespace "json-message-parsing") (includes? namespace "failed"))
      (prometheus/inc @reg :ziggurat/json-msg-parsed-failure-count common-labels)

      (and (includes? namespace "message-parsing") (includes? namespace "failed"))
      (prometheus/inc @reg :ziggurat/msg-parsed-failure-count common-labels)

      (includes? namespace "message-processing")
      (prometheus/inc @reg :ziggurat/msg-processed-count (assoc common-labels :code metric))

      (and (includes? namespace "message"))
      (prometheus/inc @reg :ziggurat/msg-read-count (assoc common-labels :code metric))

      (includes? namespace "batch.consumption")
      (prometheus/inc @reg :ziggurat/batch-consumption-count (assoc common-labels :code metric))

      (includes? namespace "http-server")
      (prometheus/inc @reg :ziggurat/http-metrics (-> common-labels
                                         (assoc  :uri (get tags :uri))
                                         (assoc  :response (get tags :response))))

      (and (includes? namespace "rabbitmq") (includes? namespace "publish"))
      (prometheus/inc @reg :ziggurat/rabbitmq-publish (assoc common-labels :code metric))

      (and (includes? namespace "rabbitmq") (includes? namespace "conversion"))
      (prometheus/inc @reg :ziggurat/rabbitmq-read (assoc common-labels :code metric))

      (and (includes? namespace "rabbitmq") (includes? namespace "process") (includes? namespace "failure"))
      (prometheus/inc @reg :ziggurat/rabbitmq-msg-processed-failure common-labels)

      (and (includes? namespace "rabbitmq") (includes? namespace "consumption") (includes? namespace "failure"))
      (prometheus/inc @reg :ziggurat/rabbitmq-msg-consumption-failure common-labels)

      :else
      (do
        (println "PANIC" namespace metric tags value)
        (System/exit 0)))))

(defn update-counter
  [namespace metric tags value]
  (if-not (starts-with? (trim namespace) (-> (ziggurat-config) :app-name))
    (update-prom-metric namespace metric tags value)))

(defn update-timing
  [namespace metric tags value]
  (println "update-timing" "namespace: " namespace "metric" metric "tags" tags "value" value)
  (let [common-labels {:topic-name (or (get tags :topic_name) (get tags :topic-entity))
                       :actor        (-> (ziggurat-config) :app-name)
                       :env        (get tags :env)}]
    (cond
      (and (includes? namespace "ziggurat.batch.consumption") (includes? namespace "execution-time"))
      (prometheus/observe @reg :ziggurat/batch-handler-fn-execution-time common-labels value)

      (includes? namespace "stream-joins-message-diff")
      (prometheus/observe @reg :ziggurat/stream-joins-message-diff (-> common-labels
                                                                       (dissoc :topic-name)
                                                                       (assoc :left (get tags :left))
                                                                       (assoc :right (get tags :right))) value)

      (includes? namespace "execution-time") ;; includes both mapper-func and channel-mapper-func
      (prometheus/observe @reg :ziggurat/handler-fn-execution-time common-labels value)

      (includes? namespace "message-received-delay-histogram") ;; includes both mapper-func and channel-mapper-func
      (prometheus/observe @reg :ziggurat/kafka-delay-time common-labels value)

      :else
      (do
        (println "PANIC" namespace metric tags value)
        (System/exit 0))))  )

(deftype Standalone []
  MetricsProtocol
  (initialize [this prom-config] (initialize prom-config))
  (terminate [this] (terminate))
  (update-counter [this namespace metric tags signed-val] (update-counter namespace metric tags signed-val))
  (update-timing [this namespace metric tags value] (update-timing namespace metric tags value)))
