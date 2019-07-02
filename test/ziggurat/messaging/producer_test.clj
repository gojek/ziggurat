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
            [ziggurat.config :as config]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest retry-for-channel-test
  (testing "message in channel will be retried as defined in ziggurat config channel retry when message doesnt have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1 #(constantly nil)}}
      (let [topic-entity :default
            channel :channel-1
            retry-count (atom (-> (ziggurat-config) :stream-router topic-entity :channels channel :retry :count))
            message-payload      {:message {:foo "bar"} :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)]
        (producer/retry-for-channel message-payload topic-entity channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq topic-entity channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message in channel will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1 #(constantly nil)}}
      (let [retry-count (atom 2)
            topic-entity :default
            channel :channel-1
            message-payload {:message {:foo "bar"} :retry-count @retry-count}
            expected-message-payload {:message {:foo "bar"} :retry-count 0}]
        (producer/retry-for-channel message-payload topic-entity channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (producer/retry-for-channel message-from-mq topic-entity channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= expected-message-payload message-from-mq)))))))

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [message-payload          {:message {:foo "bar"} :retry-count 5}
            expected-message-payload {:message {:foo "bar"} :retry-count 4}
            topic-entity     :default]
        (producer/retry message-payload topic-entity)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [message-payload  {:message {:foo "bar"} :retry-count 0}
            topic-entity     :default]
        (producer/retry message-payload topic-entity)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= message-payload message-from-mq)))))

    (testing "it will retry publishing message six times when unable to publish to rabbitmq"
      (fix/with-queues
        {:default {:handler-fn #(constantly nil)}}
        (let [retry-count (atom 0)
              message-payload  {:message {:foo "bar"} :retry-count 5}
              topic-entity     :default]
          (with-redefs [lb/publish (fn [_ _ _ _ props]
                                     (swap! retry-count inc)
                                     (throw (Exception. "some exception")))]
            (producer/retry message-payload topic-entity)
            (is (= 6 @retry-count)))))))

  (testing "message with no retry count will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [message          {:message {:foo "bar"} :retry-count (:count (:retry (ziggurat-config)))}
            expected-message {:message {:foo "bar"} :retry-count 4}
            topic-entity     :default]
        (producer/retry message topic-entity)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= message-from-mq expected-message))))))

  (testing "publish to delay queue publishes with expiration from config"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [message-payload        {:message {:foo "bar"} :retry-count (:count (:retry (ziggurat-config)))}
            topic-entity   :default
            expected-props {:content-type "application/octet-stream"
                            :persistent   true
                            :expiration   (str (get-in (rabbitmq-config) [:delay :queue-timeout-ms]))}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue topic-entity message-payload)))))

  (testing "message will be retried as defined in ziggurat config retry-count when message doesnt have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count (atom (get-in (config/ziggurat-config) [:retry :count]))
            message-payload     {:message {:foo "bar"} :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)
            topic-entity :default]
        (producer/retry message-payload topic-entity)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (producer/retry message-from-mq topic-entity)))
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= expected-message-payload message-from-mq))))))

  (testing "message will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count              (atom 2)
            message-payload          {:message {:foo "bar"} :retry-count @retry-count}
            expected-message-payload (assoc message-payload :retry-count 0)
            topic-entity             :default]
        (producer/retry message-payload topic-entity)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (producer/retry message-from-mq topic-entity)))
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
