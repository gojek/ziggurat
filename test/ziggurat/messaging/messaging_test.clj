(ns ziggurat.messaging.messaging-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.util.mock-messaging-implementation]
            [ziggurat.util.mock-messaging-implementation :as mock-messaging]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.util :as util]
            [ziggurat.config :as config]
            [ziggurat.fixtures :as fix])
  (:import (ziggurat.messaging.rabbitmq_wrapper RabbitMQMessaging)
           (ziggurat.util.mock_messaging_implementation MockMessaging)))

(defn reset-impl [] (reset! messaging/messaging-impl nil))

(use-fixtures :once  fix/mount-only-config)

(def ziggurat-config-for-mock-impl (constantly {:messaging
                                                {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}}))

(deftest initialise-messaging-library-test
  (testing "it should default to RabbitMQMessaging library if no implementation is provided"
    (with-redefs [ziggurat-config (constantly {:messaging {:constructor nil}})]
      (messaging/initialise-messaging-library)
      (is (instance? RabbitMQMessaging (deref messaging/messaging-impl)))
      (reset-impl)))

  (testing "it should initialise the messaging library as provided in the config"
    (with-redefs [ziggurat-config (constantly {:messaging
                                               {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}})]
      (messaging/initialise-messaging-library)
      (is (instance? MockMessaging (deref messaging/messaging-impl)))
      (reset-impl)))

  (testing "It raises an exception when incorrect constructor has been configured"
    (with-redefs [ziggurat-config (constantly {:messaging {:constructor "incorrect-constructor"}})]
      (is (thrown? RuntimeException (messaging/initialise-messaging-library))))))

(deftest get-implementation-test
  (testing "it should throw an Exception if `messaging-impl` atom is nil"
    (with-redefs [messaging/messaging-impl (atom nil)]
      (is (thrown? Exception (messaging/get-implementation)))))

  (testing "it should return `messaging-impl` atom if it is not nil"
    (let [atom-val {:foo "bar"}]
      (with-redefs [messaging/messaging-impl (atom atom-val)]
        (is (= atom-val (messaging/get-implementation)))
        (reset-impl)))))

(deftest start-connection-test
  (testing "it should call the mock-messaging/start-connection function with the correct arguments when is-connection-required? is true"
    (let [test-stream-routes       {:default {:handler-fn (constantly nil)}}
          start-connection-called? (atom false)]
      (with-redefs [ziggurat-config                 ziggurat-config-for-mock-impl
                    util/is-connection-required?    (fn [_ _] true)
                    mock-messaging/start-connection (fn [config stream-routes]
                                                      (when (and (= config config/config)
                                                                 (= test-stream-routes stream-routes))
                                                        (reset! start-connection-called? true)))]
        (messaging/start-connection config/config test-stream-routes)
        (is (= true @start-connection-called?))
        (reset-impl))))

  (testing "it should not call the mock-messaging/start-connection function with the correct arguments when is-connection-required? is false"
    (let [test-stream-routes       {:default {:handler-fn (constantly nil)}}
          start-connection-called? (atom false)]
      (with-redefs [ziggurat-config                 ziggurat-config-for-mock-impl
                    util/is-connection-required? (fn [_ _] false)
                    mock-messaging/start-connection (fn [config stream-routes]
                                                      (when (and (= config config/config)
                                                                 (= test-stream-routes stream-routes))
                                                        (reset! start-connection-called? true)))]
        (messaging/start-connection config/config test-stream-routes)
        (is (= false @start-connection-called?))
        (reset-impl))))

  (testing "if retry is disabled and channels are not present it should not start the connection"
    (let [start-connection-called?       (atom false)
          stream-routes             {:default {:handler-fn (constantly :success)}}
          overridden-default-config (assoc config/config
                                           :ziggurat (-> (ziggurat-config)
                                                         (assoc :messaging {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"})
                                                         (assoc :retry {:enabled false})))]
      (with-redefs [mock-messaging/start-connection   (fn [_ _]
                                                        (reset! start-connection-called? true))
                    config/config overridden-default-config]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (is (not @start-connection-called?)))))

  (testing "if retry is disabled and channels are present it should start connection"
    (let [start-connection-called?       (atom false)
          stream-routes             {:default   {:handler-fn (constantly :channel-1)
                                                 :channel-1  (constantly :success)}
                                     :default-1 {:handler-fn (constantly :channel-3)
                                                 :channel-3  (constantly :success)}}
          overridden-default-config (assoc config/config
                                           :ziggurat (assoc (ziggurat-config)
                                                            :retry {:enabled false}
                                                            :messaging {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}))]
      (with-redefs [messaging/start-connection   (fn [_ _]
                                                   (reset! start-connection-called? true))
                    config/config overridden-default-config]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (is @start-connection-called?))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [start-connection-called? (atom false)
          stream-routes          {:default   {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}
                                  :default-1 {:handler-fn (constantly :channel-3)
                                              :channel-3  (constantly :success)}}
          overridden-config      (assoc config/config
                                        :ziggurat (assoc (ziggurat-config)
                                                         :messaging {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}
                                                         :retry {:enabled false}))]
      (with-redefs [mock-messaging/start-connection (fn [_ _]
                                                      (reset! start-connection-called? true))
                    config/config              overridden-config]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (is @start-connection-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [start-connection-called?       (atom false)
          stream-routes             {:default {:handler-fn (constantly :success)}}
          overridden-default-config (assoc config/config
                                           :ziggurat (assoc (ziggurat-config)
                                                            :messaging {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}
                                                            :retry {:enabled true}))]
      (with-redefs [mock-messaging/start-connection   (fn [_ _]
                                                        (reset! start-connection-called? true))
                    config/config overridden-default-config]
        (messaging/start-connection config/config stream-routes)
        (is @start-connection-called?)
        (messaging/stop-connection config/config stream-routes)))))

(deftest stop-connection-test
  (testing "it should call the mock-messaging/stop-connection function with the correct arguments"
    (let [test-stream-routes      {:default {:handler-fn (constantly nil)}}
          stop-connection-called? (atom false)]
      (with-redefs [ziggurat-config                ziggurat-config-for-mock-impl
                    mock-messaging/stop-connection (fn [config stream-routes]
                                                     (when (and (= config config/config)
                                                                (= test-stream-routes stream-routes))
                                                       (reset! stop-connection-called? true)))]
        (messaging/start-connection config/config test-stream-routes)
        (messaging/stop-connection config/config test-stream-routes)
        (is (= true @stop-connection-called?))
        (reset-impl))))

  (testing "it should not call the mock-messaging/stop-connection function when the `messaging-impl` is nil"
    (let [test-stream-routes      {:default {:handler-fn (constantly nil)}}
          stop-connection-called? (atom false)]
      (with-redefs [ziggurat-config ziggurat-config-for-mock-impl
                    messaging/messaging-impl (atom nil)
                    mock-messaging/stop-connection (fn [config stream-routes]
                                                     (when (and (= config config/config)
                                                                (= test-stream-routes stream-routes))
                                                       (reset! stop-connection-called? true)))]
        (messaging/stop-connection config/config test-stream-routes)
        (is (= false @stop-connection-called?))
        (reset-impl)))))

(deftest create-and-bind-queue-test
  (testing "it should call the mock-messaging-create-and-bind-queue function when dead-letter-exchange is not passed"
    (let [stream-routes                 {:default {:handler-fn (constantly nil)}}
          test-queue-name               "test-queue"
          test-exchange-name            "test-exchange-name"
          create-and-bind-queue-called? (atom false)]
      (with-redefs [ziggurat-config                      ziggurat-config-for-mock-impl
                    mock-messaging/create-and-bind-queue (fn [queue-name exchange-name]
                                                           (when (and (= queue-name test-queue-name)
                                                                      (= exchange-name test-exchange-name))
                                                             (reset! create-and-bind-queue-called? true)))]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/create-and-bind-queue test-queue-name test-exchange-name)
        (is (= true @create-and-bind-queue-called?))
        (reset-impl))))

  (testing "it should call the mock-messaging-create-and-bind-queue function when dead-letter-exchange is passed"
    (let [stream-routes                 {:default {:handler-fn (constantly nil)}}
          test-queue-name               "test-queue"
          test-exchange-name            "test-exchange-name"
          test-dead-letter-exchange     "test-dlx"
          create-and-bind-queue-called? (atom false)]
      (with-redefs [ziggurat-config                      ziggurat-config-for-mock-impl
                    mock-messaging/create-and-bind-queue (fn [queue-name exchange-name dead-letter-exchange]
                                                           (when (and (= queue-name test-queue-name)
                                                                      (= exchange-name test-exchange-name)
                                                                      (= dead-letter-exchange test-dead-letter-exchange))
                                                             (reset! create-and-bind-queue-called? true)))]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/create-and-bind-queue test-queue-name test-exchange-name test-dead-letter-exchange)
        (is (= true @create-and-bind-queue-called?))
        (reset-impl)))))

(deftest publish-test
  (testing "it should call `mock-messaging/publish` without expiration"
    (let [test-exchange-name   "test-exchange"
          stream-routes        {:default {:handler-fn (constantly nil)}}
          test-message-payload {:foo "bar"}
          publish-called?      (atom false)]
      (with-redefs [mock-messaging/publish (fn [exchange message-payload]
                                             (when (and (= exchange test-exchange-name)
                                                        (= message-payload test-message-payload))
                                               (reset! publish-called? true)))
                    ziggurat-config        ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/publish test-exchange-name test-message-payload)
        (is (= @publish-called? true))
        (reset-impl))))

  (testing "It should call `mock-messaging/publish` with expiration"
    (let [test-exchange-name   "test-exchange"
          stream-routes        {:default {:handler-fn (constantly nil)}}
          test-message-payload {:foo "bar"}
          test-expiration      "42"
          publish-called?      (atom false)]
      (with-redefs [mock-messaging/publish (fn [exchange message-payload expiration]
                                             (when (and (= exchange test-exchange-name)
                                                        (= message-payload test-message-payload)
                                                        (= expiration test-expiration))
                                               (reset! publish-called? true)))
                    ziggurat-config        ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/publish test-exchange-name test-message-payload test-expiration)
        (is (= @publish-called? true))
        (reset-impl)))))

(deftest get-messages-from-queue-test
  (testing "it should call `mock-messaging/get-messages-from-queue` without `count`"
    (let [test-queue-name                 "test-queue"
          stream-routes                   {:default {:handler-fn (constantly nil)}}
          get-messages-from-queue-called? (atom false)]
      (with-redefs [mock-messaging/get-messages-from-queue (fn [queue-name ack?]
                                                             (when (and (= test-queue-name queue-name)
                                                                        (= ack? true)))
                                                             (reset! get-messages-from-queue-called? true))
                    ziggurat-config                        ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/get-messages-from-queue test-queue-name true)
        (reset-impl))))

  (testing "it should call `mock-messaging/get-messages-from-queue` when `count` is specified"
    (let [test-queue-name                 "test-queue"
          test-count                      5
          stream-routes                   {:default {:handler-fn (constantly nil)}}
          get-messages-from-queue-called? (atom false)]
      (with-redefs [mock-messaging/get-messages-from-queue (fn [queue-name ack? count]
                                                             (when (and (= test-queue-name queue-name)
                                                                        (= test-count count)
                                                                        (= ack? true)))
                                                             (reset! get-messages-from-queue-called? true))
                    ziggurat-config                        ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/get-messages-from-queue test-queue-name true test-count)
        (reset-impl)))))

(deftest process-messages-from-queue-test
  (testing "It should call `mock-messaging/process-messages-from-queue` with the correct arguments"
    (let [test-queue                          "test-queue"
          test-count                          5
          test-processing-fn                  (constantly {:foo "bar"})
          stream-routes                       {:default {:handler-fn (constantly nil)}}
          process-messages-from-queue-called? (atom false)]
      (with-redefs [mock-messaging/process-messages-from-queue (fn [queue-name count processing-fn]
                                                                 (when (and (= queue-name test-queue)
                                                                            (= count test-count)
                                                                            (= {:foo "bar"} (processing-fn)))
                                                                   (reset! process-messages-from-queue-called? true)))
                    ziggurat-config                            ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/process-messages-from-queue test-queue test-count test-processing-fn)
        (is (= true @process-messages-from-queue-called?))
        (reset-impl)))))

(deftest start-subscriber-test
  (testing "it should call `mock-messaging/start-subscriber` with the right arguments"
    (let [test-prefetch-count      5
          test-queue-name          "test-queue"
          test-mapper-fn           (constantly {:foo "bar"})
          stream-routes            {:default {:handler-fn (constantly nil)}}
          start-subscriber-called? (atom false)]
      (with-redefs [mock-messaging/start-subscriber (fn [prefetch-count wrapped-mapper-fn queue-name]
                                                      (when (and (= queue-name test-queue-name)
                                                                 (= test-prefetch-count prefetch-count)
                                                                 (= (wrapped-mapper-fn) {:foo "bar"}))
                                                        (reset! start-subscriber-called? true)))
                    ziggurat-config                 ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/start-subscriber test-prefetch-count test-mapper-fn test-queue-name)
        (reset-impl)))))

(deftest consume-message-test
  (testing "it should call `mock-messaging/consume-message` with the correct arguments"
    (let [test-meta     {:meta "bar"}
          test-payload  {:foo "bar"}
          test-ack?     false
          stream-routes {:default {:handler-fn (constantly nil)}}]
      (with-redefs [mock-messaging/consume-message (fn [_ meta payload ack?]
                                                     (when (and (= meta test-meta)
                                                                (= payload test-payload)
                                                                (= ack? test-ack?))))
                    ziggurat-config                ziggurat-config-for-mock-impl]
        (messaging/start-connection config/config stream-routes)
        (messaging/stop-connection config/config stream-routes)
        (messaging/consume-message nil test-meta test-payload test-ack?)
        (reset-impl)))))

