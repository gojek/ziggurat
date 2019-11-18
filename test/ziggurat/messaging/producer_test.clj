(ns ziggurat.messaging.producer-test
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [ziggurat.config :refer [rabbitmq-config ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :as util]
            [ziggurat.util.rabbitmq :as rmq]
            [langohr.basic :as lb]
            [ziggurat.config :as config]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.mapper :refer [->MessagePayload]])
  (:import [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest retry-for-channel-test
  (testing "message in channel will be retried as defined in ziggurat config channel retry when message doesn't have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1 #(constantly nil)}}
      (let [topic-entity :default
            channel :channel-1
            retry-count (atom (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :count))
            message-payload      {:message {:foo "bar"} :topic-entity topic-entity}
            expected-message-payload (assoc message-payload :retry-count 0)]
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
                 :channel-1 #(constantly nil)}}
      (let [retry-count (atom 2)
            topic-entity :default
            channel :channel-1
            message-payload {:message {:foo "bar"} :retry-count @retry-count :topic-entity topic-entity}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry-for-channel message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried with linear queue timeout"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :linear-retry #(constantly nil)}}
      (let [retry-count (atom 2)
            topic-entity :default
            channel :linear-retry
            message-payload {:message {:foo "bar"}  :topic-entity topic-entity :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry-for-channel message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried with exponential queue timeout"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :exponential-retry #(constantly nil)}}
      (let [retry-count (atom 2)
            topic-entity :default
            channel :exponential-retry
            message-payload {:message {:foo "bar"}  :topic-entity topic-entity :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry-for-channel message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried with channel exponential queue timeout"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-exponential-retry #(constantly nil)}}
      (let [retry-count (atom 2)
            topic-entity :default
            channel :channel-exponential-retry
            message-payload {:message {:foo "bar"}  :topic-entity topic-entity :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry-for-channel message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq)))))))

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [topic-entity             :default
            message-payload          {:message {:foo "bar"} :retry-count 5 :topic-entity topic-entity}
            expected-message-payload (update message-payload :retry-count dec)]
        (producer/retry message-payload)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [topic-entity    :default
            message-payload {:message {:foo "bar"} :retry-count 0 :topic-entity topic-entity}]
        (producer/retry message-payload)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= message-payload message-from-mq))))))

  (testing "it will retry publishing message six times when unable to publish to rabbitmq"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count     (atom 0)
            topic-entity    :default
            message-payload {:message {:foo "bar"} :retry-count 5 :topic-entity topic-entity}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (swap! retry-count inc)
                                   (throw (Exception. "some exception")))]
          (producer/retry message-payload)
          (is (= 6 @retry-count))))))

  (testing "message with no retry count will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [topic-entity     :default
            message-payload  {:message {:foo "bar"} :topic-entity topic-entity}
            expected-message (assoc message-payload :retry-count 4)]
        (producer/retry message-payload)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= message-from-mq expected-message))))))

  (testing "publish to delay queue publishes with expiration from config"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [topic-entity    :default
            message-payload {:message {:foo "bar"} :topic-entity topic-entity}
            expected-props  {:content-type "application/octet-stream"
                             :persistent   true
                             :expiration   (str (get-in (rabbitmq-config) [:delay :queue-timeout-ms]))
                             :headers      {}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue message-payload)))))

  (testing "publish to delay queue publishes with parsed record headers"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [topic-entity    :default
            message-payload {:message {:foo "bar"} :topic-entity topic-entity :headers (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value")))))}
            expected-props {:content-type "application/octet-stream"
                            :persistent   true
                            :expiration   (str (get-in (rabbitmq-config) [:delay :queue-timeout-ms]))
                            :headers      {"key" "value"}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue message-payload)))))

  (testing "message will be retried as defined in ziggurat config retry-count when message doesn't have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count (atom (get-in (config/ziggurat-config) [:retry :count]))
            topic-entity :default
            message-payload     {:message {:foo "bar"} :topic-entity topic-entity}
            expected-message-payload (assoc message-payload :retry-count 0)]
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
            topic-entity             :default
            message-payload          {:message {:foo "bar"} :retry-count @retry-count :topic-entity topic-entity}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry message-payload)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (producer/retry message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= expected-message-payload message-from-mq)))))))

(deftest make-queues-test
  (let [ziggurat-config (ziggurat-config)]
    (testing "When retries are enabled"
      (with-redefs [ziggurat-config (constantly (assoc ziggurat-config
                                                       :retry {:enabled true}))]
        (testing "it does not create queues when stream-routes are not passed"
          (let [counter (atom 0)]
            (with-redefs [producer/create-and-bind-queue (fn
                                                           ([_ _] (swap! counter inc))
                                                           ([_ _ _] (swap! counter inc)))]
              (producer/make-queues nil)
              (producer/make-queues [])
              (is (= 0 @counter)))))

        (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
          (let [counter       (atom 0)
                stream-routes {:test  {:handler-fn #(constantly nil)}
                               :test2 {:handler-fn #(constantly nil)}}]
            (with-redefs [producer/create-and-bind-queue (fn
                                                           ([_ _] (swap! counter inc))
                                                           ([_ _ _] (swap! counter inc)))]
              (producer/make-queues stream-routes)
              (is (= (* (count stream-routes) 3) @counter)))))

        (testing "it calls create queue for each queue creation with appropriate props"
          (let [counter               (atom 0)
                stream-routes         {:default {:handler-fn #(constantly nil)}}
                instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))]
            (with-redefs [producer/create-queue           (fn [queue props _]
                                                            (swap! counter inc)
                                                            (cond
                                                              (= queue instant-queue-name) (is (empty? (:arguments props)))
                                                              (= queue delay-queue-name) (is (= {"x-dead-letter-exchange" instant-exchange-name} props))
                                                              (= queue dead-queue-name) (is (empty? (:arguments props)))))
                          producer/declare-exchange       (fn [_ _] true)
                          producer/bind-queue-to-exchange (fn [_ _ _] true)]
              (producer/make-queues stream-routes)
              (is (= (* (count stream-routes) 3) @counter)))))

        (testing "it creates queues with topic entity from stream routes"
          (with-open [ch (lch/open connection)]
            (let [stream-routes         {:default {:handler-fn #(constantly :success)}}

                  instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))

                  delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name   (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))

                  dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name    (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))

                  expected-queue-status {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)

              (is (= expected-queue-status (lq/status ch instant-queue-name)))
              (is (= expected-queue-status (lq/status ch delay-queue-name)))
              (is (= expected-queue-status (lq/status ch dead-queue-name)))

              (lq/delete ch instant-queue-name)
              (lq/delete ch delay-queue-name)
              (lq/delete ch dead-queue-name)
              (le/delete ch delay-exchange-name)
              (le/delete ch instant-exchange-name)
              (le/delete ch dead-exchange-name))))))

    (testing "when retries are disabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (testing "it does not create queues when stream-routes are not passed"
          (let [counter (atom 0)]
            (with-redefs [producer/create-and-bind-queue (fn
                                                           ([_ _] (swap! counter inc))
                                                           ([_ _ _] (swap! counter inc)))]
              (producer/make-queues {:default {:handler-fn #(constantly :success)}})
              (is (= 0 @counter)))))

        (testing "it creates queues with topic entity for channels only"
          (with-open [ch (lch/open connection)]
            (let [stream-routes                  {:default {:handler-fn #(constantly :success) :channel-1 #(constantly :success)}}
                  instant-queue-suffix           (:queue-name (:instant (rabbitmq-config)))
                  instant-exchange-suffix        (:exchange-name (:instant (rabbitmq-config)))
                  delay-queue-suffix             (:queue-name (:delay (rabbitmq-config)))
                  delay-exchange-suffix          (:exchange-name (:delay (rabbitmq-config)))
                  dead-letter-queue-suffix       (:queue-name (:dead-letter (rabbitmq-config)))
                  dead-letter-exchange-suffix    (:exchange-name (:dead-letter (rabbitmq-config)))
                  prefix-name                    "default_channel_channel-1"
                  channel1-instant-queue-name    (util/prefixed-queue-name prefix-name instant-queue-suffix)
                  channel1-instant-exchange-name (util/prefixed-queue-name prefix-name instant-exchange-suffix)
                  channel1-delay-queue-name      (util/prefixed-queue-name prefix-name delay-queue-suffix)
                  channel1-delay-exchange-name   (util/prefixed-queue-name prefix-name delay-exchange-suffix)
                  channel1-dead-queue-name       (util/prefixed-queue-name prefix-name dead-letter-queue-suffix)
                  channel1-dead-exchange-name    (util/prefixed-queue-name prefix-name dead-letter-exchange-suffix)
                  expected-queue-status          {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)
              (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
              (is (= expected-queue-status (lq/status ch channel1-delay-queue-name)))
              (is (= expected-queue-status (lq/status ch channel1-dead-queue-name)))

              (lq/delete ch channel1-instant-queue-name)
              (lq/delete ch channel1-delay-queue-name)
              (lq/delete ch channel1-dead-queue-name)
              (le/delete ch channel1-delay-exchange-name)
              (le/delete ch channel1-instant-exchange-name)
              (le/delete ch channel1-dead-exchange-name))))))

    (testing "when retries are disabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}
                                                              :stream-router {:default {:channels {:channel-1 {:retry {:enabled false}}}}}))]

        (testing "it creates instant queues with topic entity for channels only"
          (with-open [ch (lch/open connection)]
            (let [stream-routes                  {:default {:handler-fn #(constantly :success) :channel-1 #(constantly :success)}}
                  instant-queue-suffix           (:queue-name (:instant (rabbitmq-config)))
                  instant-exchange-suffix        (:exchange-name (:instant (rabbitmq-config)))
                  prefix-name                    "default_channel_channel-1"
                  channel1-instant-queue-name    (util/prefixed-queue-name prefix-name instant-queue-suffix)
                  channel1-instant-exchange-name (util/prefixed-queue-name prefix-name instant-exchange-suffix)
                  expected-queue-status          {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)
              (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
              (lq/delete ch channel1-instant-queue-name)
              (le/delete ch channel1-instant-exchange-name))))))))

(deftest publish-to-instant-queue-test
  (testing "given a message-payload, it publishes it to the correct queue"
    (let [expected-exchange-name   "exchange"
          expected-topic-entity    :topic
          prefixed-queue-name-called? (atom false)
          publish-called?             (atom false)
          message-payload          {:message      {:foo "bar"}
                                    :topic-entity expected-topic-entity
                                    :retry-count  0}]
      (with-redefs [rabbitmq-config (constantly {:instant {:exchange-name expected-exchange-name}})
                    util/prefixed-queue-name (fn [topic-entity exchange]
                                               (if (and (= topic-entity expected-topic-entity)
                                                        (= exchange expected-exchange-name))
                                                 (reset! prefixed-queue-name-called? true))
                                               expected-exchange-name)
                    producer/publish (fn [exchange message]
                                       (if (and (= exchange expected-exchange-name)
                                                (= message message-payload))
                                         (reset! publish-called? true)))]
        (producer/publish-to-instant-queue message-payload)
        (is (true? @prefixed-queue-name-called?))
        (is (true? @publish-called?))))))

(deftest publish-to-delay-queue-test
  (testing "creates a span when tracer is enabled"
    (let [stream-routes {:default {:handler-fn #(constantly nil)
                                   :channel-1  #(constantly nil)}}]
      (.reset tracer)
      (fix/with-queues
        stream-routes
        (let [topic-entity :default
              message-payload {:message {:foo "bar"} :topic-entity topic-entity}]
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
        (let [topic-entity :default
              message-payload {:message {:foo "bar"} :topic-entity topic-entity}]
          (producer/publish-to-channel-instant-queue :channel-1 message-payload)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 1 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))))))))

(deftest get-queue-timeout-ms-test
  (let [message (assoc (->MessagePayload "message" "topic-entity") :retry-count 2)]
    (testing "when retries are enabled"
      (let [topic-entity :default
            channel :linear-retry]
        (is (= 2000 (producer/get-queue-timeout-ms topic-entity channel message)))))))
    ;(testing "when exponential backoff are enabled and channel retry count not defined"
    ;  (let [topic-entity :default
    ;        channel :channel-no-retry-count]
    ;    (is (= 700 (producer/get-queue-timeout-ms topic-entity channel message)))))
    ;(testing "when exponential backoff are enabled and channel queue timeout defined"
    ;  (let [topic-entity :default
    ;        channel :exponential-retry]
    ;    (is (= 7000 (producer/get-queue-timeout-ms topic-entity channel message)))))
    ;(testing "when exponential backoff are enabled and channel queue timeout not defined"
    ;  (let [topic-entity :default
    ;        channel :channel-exponential-retry]
    ;    (is (= 700 (producer/get-queue-timeout-ms topic-entity channel message)))))))
