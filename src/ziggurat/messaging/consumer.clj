(ns ziggurat.messaging.consumer
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.mapper :as mpr]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.messaging.util :refer [get-name-with-prefix-topic]]
            [langohr.basic :as lb]
            [langohr.channel :as lch]
            [langohr.consumers :as lcons]
            [mount.core :refer [defstate]]
            [taoensso.nippy :as nippy]
            [sentry.core :as sentry])
  (:import [com.rabbitmq.client AlreadyClosedException Channel]))

(defn convert-and-ack-message
  "Take the ch metadata payload and ack? as parameter. Decodes the payload the ack it if ack is enabled and returns the message"
  [ch {:keys [delivery-tag] :as meta} ^bytes payload ack?]
  (try
    (let [message (nippy/thaw payload)]
      (log/debug "Calling mapper fn with the message - " message " with retry count - " (:retry-count message))
      (if ack?
        (lb/ack ch delivery-tag))
      message)
    (catch Exception e
      (sentry/report-error sentry-reporter e "Error while decoding message")
      (lb/reject ch delivery-tag false)
      nil)))

(defn- message-handler [mapper-fn topic-name]
  (fn [ch meta ^bytes payload]
    (if-let [message (convert-and-ack-message ch meta payload true)]
      ((mpr/mapper-func mapper-fn) message topic-name))))

(defn get-queue-name
  [topic-name]
  (let [queue-name (:queue-name (:instant (:rabbit-mq (ziggurat-config))))]
    (get-name-with-prefix-topic topic-name queue-name)))

(defn get-dead-set-messages
  "Get the n(count) messages from the rabbitmq and if ack is set to true then
  ack all the messages in while consuming so that it's not available for other subscriber else does not ack the message"
  [ack? topic-name count]
  (remove nil?
          (with-open [ch (lch/open connection)]
            (doall (for [_ (range count)]
                     (try
                       (let [{:keys [queue-name]} (:dead-letter (:rabbit-mq (ziggurat-config)))
                             queue-name (get-name-with-prefix-topic topic-name queue-name)
                             [meta payload] (lb/get ch queue-name false)]
                         (if (some? payload) (convert-and-ack-message ch meta payload ack?)))
                       (catch Exception e
                         (sentry/report-error sentry-reporter e "Error while consuming the dead set message"))))))))

(defn close [^Channel channel]
  (try
    (.close channel)
    (catch AlreadyClosedException _
      nil)))

(defn start-subscriber* [ch mapper-fn topic-name]
  (lb/qos ch (:prefetch-count (:instant (:jobs (ziggurat-config)))))
  (let [consumer-tag (lcons/subscribe ch
                                      (get-queue-name topic-name)
                                      (message-handler mapper-fn topic-name)
                                      {:handle-shutdown-signal-fn (fn [consumer_tag reason]
                                                                    (log/info "Closing channel with consumer tag - " consumer_tag)
                                                                    (close ch))})]

    (log/info "starting consumer for instant-queue with consumer tag - " consumer-tag)))



(defn start-subscribers
  "Starts the subscriber to the instant queue of the rabbitmq"
  [mapper-fn stream-routes]
  (when (-> (ziggurat-config) :retry :enabled)
    (let [workers (:worker-count (:instant (:jobs (ziggurat-config))))]
      (doseq [worker (range workers)]
        (if (nil? stream-routes)
          (start-subscriber* (lch/open connection) mapper-fn nil)
          (doseq [[key value] stream-routes]
            (start-subscriber* (lch/open connection) (:handler-fn value) (name key))))))))

