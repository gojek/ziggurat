(ns ziggurat.messaging.rabbitmq.cluster.producer
  (:require [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.http :as lh]
            [langohr.exchange :as le]
            [ziggurat.messaging.rabbitmq.retry :refer :all]
            [langohr.queue :as lq]
            [clojure.string :as str])
  (:import (org.apache.kafka.common.header.internals RecordHeader)))

(defn get-replica-count [host-count]
  (int (Math/ceil (/ host-count 2))))

(defn get-default-ha-policy [cluster-config replica-count]
  (let [ha-mode      (get cluster-config :ha-mode "exactly")
        ha-params    (get cluster-config :ha-params  replica-count)
        ha-sync-mode (get cluster-config :ha-sync-mode "automatic")]
    (if (= "all" ha-mode)
      {:ha-mode "all" :ha-sync-mode ha-sync-mode}
      {:ha-mode ha-mode :ha-sync-mode ha-sync-mode :ha-params ha-params})))

(defn set-ha-policy [queue-name exchange-name cluster-config]
  (let [hosts-vec (str/split (:hosts cluster-config) #",")
        hosts     (atom hosts-vec)]
    (with-retry {:count      (count @hosts)
                 :wait       50
                 :on-failure #(log/error "setting ha-policies failed " (.getMessage %))}
      (let [host      (first @hosts)
            _         (swap! hosts rest)
            ha-policy (get-default-ha-policy cluster-config (get-replica-count (count hosts-vec)))]
        (binding [lh/*endpoint* (str "http://" host ":" (get cluster-config :admin-port 15672))
                  lh/*username* (:username cluster-config)
                  lh/*password* (:password cluster-config)]
          (log/info "applying HA policies to queue: " queue-name)
          (log/info "applying HA policies to exchange: " exchange-name)
          (lh/set-policy "/" (str queue-name "_ha_policy")
                         {:apply-to   "all"
                          :pattern    (str "^" queue-name "|" exchange-name "$")
                          :definition ha-policy}))))))

(defn- declare-exchange [ch exchange]
  (le/declare ch exchange "fanout" {:durable true :auto-delete false})
  (log/info "Declared exchange - " exchange))

(defn- create-queue [queue props ch]
  (lq/declare ch queue {:durable true :arguments props :auto-delete false})
  (log/info "Created queue - " queue))

(defn- bind-queue-to-exchange [ch queue exchange]
  (lq/bind ch queue exchange)
  (log/infof "Bound queue %s to exchange %s" queue exchange))

(defn create-and-bind-queue
  ([cluster-config connection queue-name exchange-name dead-letter-exchange]
   (try
     (let [props (if dead-letter-exchange
                   {"x-dead-letter-exchange" dead-letter-exchange}
                   {})]
       (let [ch (lch/open connection)]
         (create-queue queue-name props ch)
         (declare-exchange ch exchange-name)
         (bind-queue-to-exchange ch queue-name exchange-name)
         (set-ha-policy queue-name exchange-name cluster-config)))
     (catch Exception e
       (log/error e "Error while declaring RabbitMQ queues")
       (throw e)))))
