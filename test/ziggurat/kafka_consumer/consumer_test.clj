(ns ziggurat.kafka-consumer.consumer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.kafka-consumer.consumer :refer [create-consumer]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer)))

(use-fixtures :once fix/mount-only-config)

(deftest create-consumer-test
  (testing "create the consumer with provided config and subscribe to provided topic"
    (let [consumer ^KafkaConsumer (create-consumer :consumer-1 (get-in (ziggurat-config) [:batch-routes :consumer-1]))
          expected-origin-topic   (get-in (ziggurat-config) [:batch-routes :consumer-1 :origin-topic])]
      (is (contains? (set (keys (.listTopics consumer))) expected-origin-topic))
      (.unsubscribe consumer)
      (.close consumer)))
  (testing "returns nil when invalid configs are provided (KafkaConsumer throws Exception)"
    (let [consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])]
      (is (= nil (create-consumer :consumer-1 (assoc-in consumer-config [:consumer-group-id] nil)))))))
