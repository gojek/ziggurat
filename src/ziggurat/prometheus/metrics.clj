(ns ziggurat.prometheus.metrics
  (:require [iapetos.core :as prometheus]))

(def bucket-sizes [0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10])

(defn all
  []
  [
   (prometheus/counter
    :ziggurat/msg-parsed-failure-count
    {:description "total msgs parsed failed by topic-entity"
     :labels      [:topic-name :actor :env]})

   (prometheus/counter
    :ziggurat/msg-read-count
    {:description "total msgs parsed failed by topic-entity"
     :labels      [:topic-name :actor :env]})

   (prometheus/counter
    :ziggurat/json-msg-parsed-failure-count
    {:description "total json msgs parsed by topic-entity"
     :labels      [:topic-name :actor :env]})

   (prometheus/counter
    :ziggurat/msg-processed-count
    {:description "total processed msg by topic-entity, status"
     :labels      [:topic-name :actor :env :code]})

   (prometheus/counter
    :ziggurat/batch-consumption-count
    {:description "total thread pool shutdown count for batch consumtion"
     :labels      [:topic-name :actor :env :code]})
   (prometheus/counter
    :ziggurat/http-metrics
    {:description "http metrics"
     :labels      [:topic-name :actor :env :uri :response]})

   (prometheus/counter
    :ziggurat/rabbitmq-publish
    {:description "rabbitmq-msg-processed"
     :labels      [:topic-name :actor :env :code]})

   (prometheus/counter
    :ziggurat/rabbitmq-read
    {:description "rabbitmq-msg-read"
     :labels      [:topic-name :actor :env :code]})

   (prometheus/counter
    :ziggurat/channel-msg-processed-count
    {:description "total processed msg by topic-entity, status"
     :labels      [:topic-name :actor :env]})

   (prometheus/counter
    :ziggurat/rabbitmq-msg-processed-failure
    {:description "rabbitmq-msg-processed"
     :labels      [:topic-name :actor :env]})

   (prometheus/counter
    :ziggurat/rabbitmq-msg-consumption-failure
    {:description "rabbitmq-msg-processed"
     :labels      [:topic-name :actor :env]})

   (prometheus/histogram
    :ziggurat/handler-fn-execution-time
    {:description "handler function execution latency by topic name"
     :labels [:topic-name :actor :env]
     :buckets bucket-sizes})

   (prometheus/histogram
    :ziggurat/batch-handler-fn-execution-time
    {:description "handler function execution latency by topic name"
     :labels [:topic-name :actor :env]
     :buckets bucket-sizes})

   (prometheus/histogram
    :ziggurat/stream-joins-message-diff
    {:description "handler function execution latency by topic name"
     :labels [:left :right]
     :buckets bucket-sizes})

   (prometheus/histogram
    :ziggurat/kafka-delay-time
    {:description "handler function execution latency by topic name"
     :labels [:topic-name]
     :buckets bucket-sizes})])
