(ns ziggurat.messaging.producer
  (:require [clojure.tools.logging :as log]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config channel-retry-config]]
            [ziggurat.messaging.util :refer :all]
            [ziggurat.retry :refer [with-retry]]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.messaging.util :as util]
            [ziggurat.sentry :refer [sentry-reporter]]))

(def MAX_EXPONENTIAL_RETRIES 25)

(defn delay-queue-name [topic-entity queue-name]
  (prefixed-queue-name topic-entity queue-name))

(defn- retry-type []
  (-> (ziggurat-config) :retry :type))

(defn- channel-retries-enabled [topic-entity channel]
  (:enabled (channel-retry-config topic-entity channel)))

(defn- channel-retry-type [topic-entity channel]
  (:type (channel-retry-config topic-entity channel)))

(defn- get-channel-retry-count [topic-entity channel]
  (:count (channel-retry-config topic-entity channel)))

(defn- get-channel-queue-timeout-or-default-timeout [topic-entity channel]
  (let [channel-queue-timeout-ms (:queue-timeout-ms (channel-retry-config topic-entity channel))
        queue-timeout-ms         (get-in (rabbitmq-config) [:delay :queue-timeout-ms])]
    (or channel-queue-timeout-ms queue-timeout-ms)))

(defn- get-backoff-exponent [retry-count message-retry-count]
  "Calculates the exponent using the formula `retry-count` and `message-retry-count`, where `retry-count` is the total retries
   possible and `message-retry-count` is the count of retries available for the message.

   Caps the value of `retry-count` to MAX_EXPONENTIAL_RETRIES.

   Returns 1, if `message-retry-count` is higher than `max(MAX_EXPONENTIAL_RETRIES, retry-count)`."
  (let [exponent (- (min MAX_EXPONENTIAL_RETRIES retry-count) message-retry-count)]
    (max 1 exponent)))

(defn- get-exponential-backoff-timeout-ms "Calculates the exponential timeout value from the number of max retries possible (`retry-count`),
   the number of retries available for a message (`message-retry-count`) and base timeout value (`queue-timeout-ms`).
   It uses this formula `((2^n)-1)*queue-timeout-ms`, where `n` is the current message retry-count.

   Sample config to use exponential backoff:
   {:ziggurat {:retry {:enabled true
                       :count   5
                       :type    :exponential}}}

   Sample config to use exponential backoff when using channel flow:
   {:ziggurat {:stream-router {topic-entity {:channels {channel {:retry {:count 5
                                                                         :enabled true
                                                                         :queue-timeout-ms 1000
                                                                         :type :exponential}}}}}}}

   _NOTE: Exponential backoff for channel retries is an experimental feature. It should not be used until released in a stable version._"
  [retry-count message-retry-count queue-timeout-ms]
  (let [exponential-backoff (get-backoff-exponent retry-count message-retry-count)]
    (int (* (dec (Math/pow 2 exponential-backoff)) queue-timeout-ms))))

(defn get-queue-timeout-ms [message-payload]
  "Calculate queue timeout for delay queue. Uses the value from [[get-exponential-backoff-timeout-ms]] if exponential backoff enabled."
  (let [queue-timeout-ms    (-> (rabbitmq-config) :delay :queue-timeout-ms)
        retry-count         (-> (ziggurat-config) :retry :count)
        message-retry-count (:retry-count message-payload)]
    (if (= :exponential (-> (ziggurat-config) :retry :type))
      (get-exponential-backoff-timeout-ms retry-count message-retry-count queue-timeout-ms)
      queue-timeout-ms)))

(defn get-channel-queue-timeout-ms [topic-entity channel message-payload]
  "Calculate queue timeout for channel delay queue. Uses the value from [[get-exponential-backoff-timeout-ms]] if exponential backoff enabled."
  (let [channel-queue-timeout-ms (get-channel-queue-timeout-or-default-timeout topic-entity channel)
        message-retry-count      (:retry-count message-payload)
        channel-retry-count      (get-channel-retry-count topic-entity channel)]
    (if (= :exponential (channel-retry-type topic-entity channel))
      (get-exponential-backoff-timeout-ms channel-retry-count message-retry-count channel-queue-timeout-ms)
      channel-queue-timeout-ms)))

(defn get-delay-exchange-name [topic-entity message-payload]
  "This function return delay exchange name for retry when using flow without channel. It will return exchange name with retry count as suffix if exponential backoff enabled."
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name (prefixed-queue-name topic-entity exchange-name)
        retry-count   (-> (ziggurat-config) :retry :count)]
    (if (= :exponential (-> (ziggurat-config) :retry :type))
      (let [message-retry-count (:retry-count message-payload)
            backoff-exponent    (get-backoff-exponent retry-count message-retry-count)]
        (prefixed-queue-name exchange-name backoff-exponent))
      exchange-name)))

(defn get-channel-delay-exchange-name [topic-entity channel message-payload]
  "This function return delay exchange name for retry when using channel flow. It will return exchange name with retry count as suffix if exponential backoff enabled."
  (let [{:keys [exchange-name]} (:delay (rabbitmq-config))
        exchange-name       (prefixed-channel-name topic-entity channel exchange-name)
        channel-retry-count (get-channel-retry-count topic-entity channel)]
    (if (= :exponential (channel-retry-type topic-entity channel))
      (let [message-retry-count (:retry-count message-payload)
            exponential-backoff (get-backoff-exponent channel-retry-count message-retry-count)]
        (str (name exchange-name) "_" exponential-backoff))
      exchange-name)))

(defn publish-to-delay-queue [message-payload]
  (let [topic-entity     (:topic-entity message-payload)
        exchange-name    (get-delay-exchange-name topic-entity message-payload)
        queue-timeout-ms (get-queue-timeout-ms message-payload)]
    (messaging/publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-dead-queue [message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (messaging/publish exchange-name message-payload)))

(defn publish-to-instant-queue [message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-queue-name topic-entity exchange-name)]
    (messaging/publish exchange-name message-payload)))

(defn publish-to-channel-delay-queue [channel message-payload]
  (let [topic-entity     (:topic-entity message-payload)
        exchange-name    (get-channel-delay-exchange-name topic-entity channel message-payload)
        queue-timeout-ms (get-channel-queue-timeout-ms topic-entity channel message-payload)]
    (messaging/publish exchange-name message-payload queue-timeout-ms)))

(defn publish-to-channel-dead-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:dead-letter (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (messaging/publish exchange-name message-payload)))

(defn publish-to-channel-instant-queue [channel message-payload]
  (let [{:keys [exchange-name]} (:instant (rabbitmq-config))
        topic-entity  (:topic-entity message-payload)
        exchange-name (prefixed-channel-name topic-entity channel exchange-name)]
    (messaging/publish exchange-name message-payload)))

(defn retry [{:keys [retry-count topic-entity] :as message-payload}]
  (when (-> (ziggurat-config) :retry :enabled)
    (cond
      (nil? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec (-> (ziggurat-config) :retry :count))))
      (pos? retry-count) (publish-to-delay-queue (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-dead-queue (assoc message-payload :retry-count (-> (ziggurat-config) :retry :count))))))

(defn retry-for-channel [{:keys [retry-count topic-entity] :as message-payload} channel]
  (when (channel-retries-enabled topic-entity channel)
    (cond
      (nil? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec (get-channel-retry-count topic-entity channel))))
      (pos? retry-count) (publish-to-channel-delay-queue channel (assoc message-payload :retry-count (dec retry-count)))
      (zero? retry-count) (publish-to-channel-dead-queue channel (assoc message-payload :retry-count (get-channel-retry-count topic-entity channel))))))

(defn- make-delay-queue [topic-entity]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)]
    (messaging/create-and-bind-queue queue-name exchange-name dead-letter-exchange-name)))

(defn- make-delay-queue-with-retry-count [topic-entity retry-count]
  (let [{:keys [queue-name exchange-name dead-letter-exchange]} (:delay (rabbitmq-config))
        queue-name                (delay-queue-name topic-entity queue-name)
        exchange-name             (prefixed-queue-name topic-entity exchange-name)
        dead-letter-exchange-name (prefixed-queue-name topic-entity dead-letter-exchange)
        sequence                  (min MAX_EXPONENTIAL_RETRIES (inc retry-count))]
    (doseq [s (range 1 sequence)]
      (messaging/create-and-bind-queue (prefixed-queue-name queue-name s) (prefixed-queue-name exchange-name s) dead-letter-exchange-name))))

(defn- make-channel-delay-queue-with-retry-count [topic-entity channel retry-count]
  (make-delay-queue-with-retry-count (with-channel-name topic-entity channel) retry-count))

(defn- make-channel-delay-queue [topic-entity channel]
  (make-delay-queue (with-channel-name topic-entity channel)))

(defn- make-queue [topic-identifier queue-type]
  (let [{:keys [queue-name exchange-name]} (queue-type (rabbitmq-config))
        queue-name    (prefixed-queue-name topic-identifier queue-name)
        exchange-name (prefixed-queue-name topic-identifier exchange-name)]
    (messaging/create-and-bind-queue queue-name exchange-name)))

(defn- make-channel-queue [topic-entity channel-name queue-type]
  (make-queue (with-channel-name topic-entity channel-name) queue-type))

(defn- make-channel-queues [channels topic-entity]
  (doseq [channel channels]
    (make-channel-queue topic-entity channel :instant)
    (when (channel-retries-enabled topic-entity channel)
      (make-channel-queue topic-entity channel :dead-letter)
      (let [channel-retry-type (channel-retry-type topic-entity channel)]
        (cond
          (= :exponential channel-retry-type) (do
                                                (log/warn "[Alpha Feature]: Exponential backoff based retries is an alpha feature."
                                                          "Please use it only after understanding its risks and implications."
                                                          "Its contract can change in the future releases of Ziggurat.")
                                                (make-channel-delay-queue-with-retry-count topic-entity channel (get-channel-retry-count topic-entity channel)))
          (= :linear channel-retry-type) (make-channel-delay-queue topic-entity channel)
          (nil? channel-retry-type) (do
                                      (log/warn "[Deprecation Notice]: Please note that the configuration for channel retries has changed."
                                                "Please look at the upgrade guide for details: https://github.com/gojek/ziggurat/wiki/Upgrade-guide"
                                                "Use :type to specify the type of retry mechanism in the channel config.")
                                      (make-channel-delay-queue topic-entity channel))
          :else (do
                  (log/warn "Incorrect keyword for type passed, falling back to linear backoff for channel: " channel)
                  (make-channel-delay-queue topic-entity channel)))))))

(defn make-queues [stream-routes]
  (when (util/is-connection-required? (ziggurat-config) stream-routes)
    (doseq [topic-entity (keys stream-routes)]
      (let [channels   (get-channel-names stream-routes topic-entity)
            retry-type (retry-type)]
        (make-channel-queues channels topic-entity)
        (when (-> (ziggurat-config) :retry :enabled)
          (make-queue topic-entity :instant)
          (make-queue topic-entity :dead-letter)
          (cond
            (= :exponential retry-type) (do
                                          (log/warn "[Alpha Feature]: Exponential backoff based retries is an alpha feature."
                                                    "Please use it only after understanding its risks and implications."
                                                    "Its contract can change in the future releases of Ziggurat.")
                                          (make-delay-queue-with-retry-count topic-entity (-> (ziggurat-config) :retry :count)))
            (= :linear retry-type) (make-delay-queue topic-entity)
            (nil? retry-type) (do
                                (log/warn "[Deprecation Notice]: Please note that the configuration for retries has changed."
                                          "Please look at the upgrade guide for details: https://github.com/gojek/ziggurat/wiki/Upgrade-guide"
                                          "Use :type to specify the type of retry mechanism in the config.")
                                (make-delay-queue topic-entity))
            :else (do
                    (log/warn "Incorrect keyword for type passed, falling back to linear backoff for topic Entity: " topic-entity)
                    (make-delay-queue topic-entity))))))))

