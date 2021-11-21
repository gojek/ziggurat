(ns ziggurat.prometheus.metrics
  (:require [iapetos.core :as prometheus]))

(defn all
  []
  [
   (prometheus/gauge
    :ziggurat/kafka-delay-time
    {:description "kafka delay gauge by topic-entity"
     :labels [:topic-name :app :env]})

   (prometheus/histogram
    :ziggurat/handler-fn-execution-time
    {:description "handler function execution latency by topic name"
     :labels [:topic-name :app :env]
     :buckets [100 250 500 1000 2000 5000]})

   (prometheus/histogram
    :ziggurat/handler-fn-batch-execution-time
    {:description "handler function batch execution latency by topic name"
     :labels [:topic-name :app :env]
     :buckets [1000 2500 5000 10000 20000 50000.0]})

   (prometheus/counter
    :ziggurat/json-parse-failure-count
    {:description "total json parsed failed count by topic-entity"
     :labels      [:topic-entity :app :env]})

   (prometheus/counter
    :ziggurat/kafka-msg-deserialize-failure-count
    {:description "total msgs from kafka for which deserialization failed by topic-entity"
     :labels      [:topic-name :app :env]})

   (prometheus/counter
    :ziggurat/msg-processed-count
    {:description "total processed msg by topic-entity, status"
     :labels      [:topic-name :code :app :env]})

   (prometheus/counter
    :ziggurat/polling-for-batch-failure-count
    {:description "failure count wating for batch polling by topic name"
     :labels      [:topic-name :app :env]})

   (prometheus/counter
    :ziggurat/rabbitmq-publish-count
    {:description "rabbitmq message publish count by exchange"
     :labels      [:topic-name :app :env :exchange]})

   (prometheus/counter
    :ziggurat/rabbitmq-publish-failure-count
    {:description "rabbitmq message publish failure count by exchange"
     :labels      [:topic-name :app :env :exchange]})

   (prometheus/counter
    :ziggurat/rabbitmq-read-count
    {:description "rabbitmq message read count by exchange"
     :labels      [:topic-name :app :env :queue]})

   (prometheus/counter
    :ziggurat/rabbitmq-read-failure-count
    {:description "rabbitmq message read failure count by queue"
     :labels      [:topic-name :app :env :queue]})])
