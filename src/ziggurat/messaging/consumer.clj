(ns ziggurat.messaging.consumer
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.name :refer [get-with-prepended-app-name]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [mount.core :refer [defstate]]
            [taoensso.nippy :as nippy]
            [sentry.core :as sentry])
  (:import [com.rabbitmq.client AlreadyClosedException Channel]))

(defn- convert-and-ack-message [ch {:keys [delivery-tag] :as meta} ^bytes payload]
  (try
    (let [message (nippy/thaw payload)]
      (log/debug "Calling mapper fn with the message - " message " with retry count - " (:retry-count message))
      (lb/ack ch delivery-tag)
      message)
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while decoding message")
      (lb/reject ch delivery-tag false)
      nil)))

(defn- message-handler
  [ch meta ^bytes payload]
  (if-let [message (convert-and-ack-message ch meta payload)]
    (mpr/mapper-func message)))

(defn get-dead-set-messages [count]
  (remove nil? (for [_ (range count)]
                 (try
                   (with-open [ch (lch/open connection)]
                     (let [{:keys [queue-name]} (:dead-letter (:rabbit-mq (ziggurat-config)))
                           [meta payload] (lb/get ch (get-with-prepended-app-name queue-name) false)]
                       (convert-and-ack-message ch meta payload)))
                   (catch Exception e
                     (sentry/report-error sentry-reporter e "Error while consuming the dead set message"))))))

(defn- close [^Channel channel]
  (try
    (.close channel)
    (catch AlreadyClosedException _
      nil)))

(defn- start-subscriber* []
  (let [ch (lch/open connection)
        _ (lb/qos ch (:prefetch-count (:instant (:jobs (ziggurat-config)))))
        consumer-tag (lcons/subscribe ch
                                      (get-with-prepended-app-name (:queue-name (:instant (:rabbit-mq (ziggurat-config)))))
                                      message-handler
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/info "Closing channel with consumer tag - " consumer_tag)
                                                                    (close ch))})]
    (log/info "starting consumer for instant-queue with cosumer tag - " consumer-tag)))

(defn start-subscribers []
  (when (-> (ziggurat-config) :retry :enabled)
    (let [workers (:worker-count (:instant (:jobs (ziggurat-config))))]
      (doseq [worker (range workers)]
        (start-subscriber*)))))
