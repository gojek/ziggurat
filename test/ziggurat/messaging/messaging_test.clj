(ns ziggurat.messaging.messaging-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.messaging :as messaging]
            [ziggurat.config :refer [ziggurat-config]])
  (:import (ziggurat.messaging.rabbitmq_wrapper RabbitMQMessaging)))

(deftest initialise-messaging-library-test
  (testing "it should default to RabbitMQMessaging library if no implementation is provided"
    (with-redefs [ziggurat-config (fn [] {:messaging {:constructor nil}})]
      (messaging/initialise-messaging-library)
      (is (instance? RabbitMQMessaging (deref messaging/messaging-impl))))))
