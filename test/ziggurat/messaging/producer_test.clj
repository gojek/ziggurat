(ns ziggurat.messaging.producer-test
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [ziggurat.config :refer [rabbitmq-config ziggurat-config channel-retry-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :as util]
            [ziggurat.util.rabbitmq :as rmq]
            [langohr.basic :as lb]
            [ziggurat.config :as config]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.mapper :refer [->MessagePayload]]
            [mount.core :as mount])
  (:import [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(def topic-entity :default)
(def message-payload (->MessagePayload {:foo "bar"} topic-entity))
(defn retry-count-config [] (-> (ziggurat-config) :retry :count))

(deftest retry-for-channel-test
  (testing "message in channel will be retried as defined in ziggurat config channel retry when message doesn't have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1  #(constantly nil)}}
      (let [channel                  :channel-1
            retry-count              (atom (:count (channel-retry-config topic-entity channel)))
            expected-message-payload (assoc message-payload :retry-count @retry-count)]
        (producer/retry-for-channel message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1  #(constantly nil)}}
      (let [retry-count              (atom 2)
            channel                  :channel-1
            channel-retry-count      (:count (channel-retry-config topic-entity channel))
            retry-message-payload    (assoc message-payload :retry-count @retry-count)

            expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
        (producer/retry-for-channel retry-message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried in delay queue with suffix 1 if message retry-count exceeds retry count in channel config"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router
                                                     {:default
                                                      {:channels
                                                       {:exponential-retry
                                                        {:retry {:count 5
                                                                 :enabled          true
                                                                 :type             :exponential
                                                                 :queue-timeout-ms 1000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn #(constantly nil)
                   :exponential-retry  #(constantly nil)}}
        (let [channel                  :exponential-retry
              retry-message-payload    (assoc message-payload :retry-count 10)
              expected-message-payload (assoc message-payload :retry-count 9)
              _                        (producer/retry-for-channel retry-message-payload channel)
              message-from-mq          (rmq/get-message-from-channel-retry-queue topic-entity channel 1)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried with linear queue timeout"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router {:default {:channels {:linear-retry {:retry {:count            5
                                                                                                                 :enabled          true
                                                                                                                 :queue-timeout-ms 2000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn   #(constantly nil)
                   :linear-retry #(constantly nil)}}
        (let [retry-count              (atom 2)
              channel                  :linear-retry
              channel-retry-count      (:count (channel-retry-config topic-entity channel))
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
          (producer/retry-for-channel retry-message-payload channel)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
              (producer/retry-for-channel message-from-mq channel)))
          (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
            (is (= expected-message-payload message-from-mq)))))))

  (testing "message in channel will be retried with exponential timeout calculated from channel specific queue-timeout-ms value"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router {:default {:channels {:exponential-retry {:retry {:count               5
                                                                                                                      :enabled             true
                                                                                                                      :type                :exponential
                                                                                                                      :queue-timeout-ms    1000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn        #(constantly nil)
                   :exponential-retry #(constantly nil)}}
        (let [retry-count              (atom 5)
              channel                  :exponential-retry
              channel-retry-count      (:count (channel-retry-config topic-entity channel))
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
          (producer/retry-for-channel retry-message-payload channel)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-message-from-channel-retry-queue topic-entity channel (- 5 @retry-count))]
              (producer/retry-for-channel message-from-mq channel)))
          (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
            (is (= expected-message-payload message-from-mq))))))))

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload    (assoc message-payload :retry-count 5)
            expected-message-payload (update retry-message-payload :retry-count dec)]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload     (assoc message-payload :retry-count 0)
            expected-dead-set-message (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= expected-dead-set-message message-from-mq))))))

  (testing "it will retry publishing message six times when unable to publish to rabbitmq"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count           (atom 0)
            retry-message-payload (assoc message-payload :retry-count 5)]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (swap! retry-count inc)
                                   (throw (Exception. "some exception")))]
          (is (thrown? clojure.lang.ExceptionInfo (producer/retry retry-message-payload)))
          (is (= 6 @retry-count))))))

  (testing "message with no retry count will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [expected-message (assoc message-payload :retry-count 4)]
        (producer/retry message-payload)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= message-from-mq expected-message))))))

  (testing "publish to delay queue publishes with expiration from config"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [expected-props {:content-type "application/octet-stream"
                            :persistent   true
                            :expiration   (str (get-in (rabbitmq-config) [`:delay :queue-timeout-ms]))
                            :headers      {}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue message-payload)))))

  (testing "publish to delay queue publishes with parsed record headers"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [test-message-payload (assoc message-payload :headers (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value"))))))
            expected-props       {:content-type "application/octet-stream"
                                  :persistent   true
                                  :expiration   (str (get-in (rabbitmq-config) [:delay :queue-timeout-ms]))
                                  :headers      {"key" "value"}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue test-message-payload)))))

  (testing "message will be retried as defined in ziggurat config retry-count when message doesn't have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count              (atom (retry-count-config))
            expected-message-payload (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry message-payload)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (producer/retry message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count              (atom 2)
            retry-message-payload    (assoc message-payload :retry-count @retry-count)
            expected-message-payload (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry retry-message-payload)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (producer/retry message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "[Backward Compatiblity] Messages will be retried even if retry type is not present in the config."
    (with-redefs [ziggurat-config (constantly (update-in (ziggurat-config) [:retry] dissoc :type))]
      (fix/with-queues
        {:default {:handler-fn #(constantly nil)}}
        (let [retry-count              (atom 2)
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count (retry-count-config))]
          (producer/retry retry-message-payload)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
              (producer/retry message-from-mq)))
          (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
            (is (= expected-message-payload message-from-mq))))))))

(deftest retry-with-exponential-backoff-test
  (testing "message will publish to delay with retry count queue when exponential backoff enabled"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :retry {:count 5
                                                             :enabled true
                                                             :type :exponential}))]
      (testing "message with no retry count will publish to delay queue with suffix 1"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [expected-message (assoc message-payload :retry-count 4)]
            (producer/retry message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 1)]
              (is (= message-from-mq expected-message))))))

      (testing "message with available retry counts as 4 will be published to delay queue with suffix 2"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload (assoc message-payload :retry-count 4)
                expected-message      (assoc message-payload :retry-count 3)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 2)]
              (is (= message-from-mq expected-message))))))

      (testing "message with available retry counts as 1 will be published to delay queue with suffix 5"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload (assoc message-payload :retry-count 1)
                expected-message      (assoc message-payload :retry-count 0)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 5)]
              (is (= message-from-mq expected-message))))))

      (testing "message will be retried in delay queue with suffix 1 if message retry-count exceeds retry count in config"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload    (assoc message-payload :retry-count 10)
                expected-message-payload (assoc message-payload :retry-count 9)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 1)]
              (is (= message-from-mq expected-message-payload)))))))))

(deftest publish-to-instant-queue-test
  (testing "given a message-payload, it publishes it to the correct queue"
    (let [expected-exchange-name      "exchange"
          expected-topic-entity       topic-entity
          prefixed-queue-name-called? (atom false)
          publish-called?             (atom false)
          retry-message-payload       (assoc message-payload :retry-count 0)]
      (with-redefs [rabbitmq-config          (constantly {:instant {:exchange-name expected-exchange-name}})
                    util/prefixed-queue-name (fn [topic-entity exchange]
                                               (if (and (= topic-entity expected-topic-entity)
                                                        (= exchange expected-exchange-name))
                                                 (reset! prefixed-queue-name-called? true))
                                               expected-exchange-name)
                    producer/publish         (fn [exchange message]
                                               (if (and (= exchange expected-exchange-name)
                                                        (= message retry-message-payload))
                                                 (reset! publish-called? true)))]
        (producer/publish-to-instant-queue retry-message-payload)
        (is (true? @prefixed-queue-name-called?))
        (is (true? @publish-called?)))))
  (testing "An exception is raised, if publishing to RabbitMQ fails even after retries"
    (mount/stop #'rmqw/connection)
    (is (thrown? clojure.lang.ExceptionInfo (producer/publish-to-instant-queue message-payload)))))

(deftest publish-to-delay-queue-test
  (testing "creates a span when tracer is enabled"
    (let [stream-routes {:default {:handler-fn #(constantly nil)
                                   :channel-1  #(constantly nil)}}]
      (.reset tracer)
      (fix/with-queues
        stream-routes
        (do
          (producer/retry message-payload)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 1 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))))))))

(deftest publish-to-channel-instant-queue-test
  (testing "creates a span when tracer is enabled"
    (let [stream-routes {:default {:handler-fn #(constantly nil)
                                   :channel-1  #(constantly nil)}}]
      (.reset tracer)
      (fix/with-queues
        stream-routes
        (do
          (producer/publish-to-channel-instant-queue :channel-1 message-payload)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 1 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))))))))

(deftest get-channel-queue-timeout-ms-test
  (let [message (assoc message-payload :retry-count 2)]
    (testing "when retries are enabled"
      (let [channel :linear-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count            5
                                                                                                                          :enabled          true
                                                                                                                          :queue-timeout-ms 2000}}}}}))]
          (is (= 2000 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))
    (testing "when exponential backoff are enabled and channel queue timeout defined"
      (let [channel :exponential-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count               5
                                                                                                                          :enabled             true
                                                                                                                          :type                :exponential
                                                                                                                          :queue-timeout-ms    1000}}}}}))]
          (is (= 7000 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))

    (testing "when exponential backoff are enabled and channel queue timeout is not defined"
      (let [channel :exponential-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count               5
                                                                                                                          :enabled             true
                                                                                                                          :type                :exponential}}}}}))]
          (is (= 700 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))))

(deftest get-queue-timeout-ms-test
  (testing "when exponential retries are enabled"
    (let [message (assoc message-payload :retry-count 2)]
      (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                              :retry {:enabled             true
                                                                      :count               5
                                                                      :type                :exponential}))]
        (is (= 700 (producer/get-queue-timeout-ms message))))))
  (testing "when exponential retries are enabled and retry-count exceeds 25, the max possible timeouts are calculated using 25 as the retry-count"
    (let [message (assoc message-payload :retry-count 20)]
      (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                              :retry {:enabled             true
                                                                      :count               50
                                                                      :type                :exponential}))]
        ;; For 25 max exponential retries, exponent comes to 25-20=5, which makes timeout = 100*(2^5-1) = 3100
        (is (= 3100 (producer/get-queue-timeout-ms message)))))))
