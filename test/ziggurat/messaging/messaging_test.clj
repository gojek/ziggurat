(ns ziggurat.messaging.messaging-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.util.mock-messaging-implementation]
            [ziggurat.util.mock-messaging-implementation :as mock-messaging]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.config :as config])
  (:import (ziggurat.messaging.rabbitmq_wrapper RabbitMQMessaging)
           (ziggurat.util.mock_messaging_implementation MockMessaging)))

(defn reset-impl [] (reset! messaging/messaging-impl nil))

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
  (testing "it should call the start-connection function with the correct arguments"
    (let [test-stream-routes {:default {:handler-fn (constantly nil)}}
          start-connection-called? (atom false)]
      (with-redefs [ziggurat-config                 (constantly {:messaging
                                                                 {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}})
                    mock-messaging/start-connection (fn [config stream-routes]
                                                      (when (and (= config config/config)
                                                                 (= test-stream-routes stream-routes))
                                                        (reset! start-connection-called? true)))]
        (messaging/start-connection config/config test-stream-routes)
        (is (= true @start-connection-called?))))))

