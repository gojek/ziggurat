(ns ziggurat.messaging.messaging-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.util.mock-messaging-implementation]
            [ziggurat.config :refer [ziggurat-config]])
  (:import (ziggurat.messaging.rabbitmq_wrapper RabbitMQMessaging)
           (ziggurat.util.mock_messaging_implementation MockMessaging)))

(deftest initialise-messaging-library-test
  (testing "it should default to RabbitMQMessaging library if no implementation is provided"
    (with-redefs [ziggurat-config (constantly {:messaging {:constructor nil}})]
      (messaging/initialise-messaging-library)
      (is (instance? RabbitMQMessaging (deref messaging/messaging-impl)))))

  (testing "it should initialise the messaging library as provided in the config"
    (with-redefs [ziggurat-config (constantly {:messaging
                                               {:constructor "ziggurat.util.mock-messaging-implementation/->MockMessaging"}})]
      (messaging/initialise-messaging-library)
      (is (instance? MockMessaging (deref messaging/messaging-impl)))))

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
        (is (= atom-val (messaging/get-implementation)))))))







