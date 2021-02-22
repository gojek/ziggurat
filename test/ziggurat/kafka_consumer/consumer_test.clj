(ns ziggurat.kafka-consumer.consumer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.kafka-consumer.consumer :refer [create-consumer]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer)
           (java.util Properties)))

(use-fixtures :once fix/mount-only-config)

(deftest create-consumer-test
  (testing "create the consumer with provided config and subscribe to provided topic"
    (let [consumer ^KafkaConsumer (create-consumer :consumer-1 (get-in (ziggurat-config) [:batch-routes :consumer-1]))
          expected-origin-topic   (get-in (ziggurat-config) [:batch-routes :consumer-1 :origin-topic])]
      (is (contains? (set (keys (.listTopics consumer))) expected-origin-topic))
      (.unsubscribe consumer)
      (.close consumer)))
  (testing "uses the config supplied by the user while creating the consumer"
    (let [ziggurat-consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])
          consumer-properties (#'ziggurat.kafka-consumer.consumer/build-consumer-properties-map ziggurat-consumer-config)]
      (with-redefs [ziggurat.kafka-consumer.consumer/build-consumer-properties-map (fn [consumer-config] (is (= (:commit-interval-ms consumer-config) (:commit-interval-ms ziggurat-consumer-config)))
                                                                                     consumer-properties)]
        (.close (create-consumer :consumer-1 ziggurat-consumer-config)))))
  (testing "uses the default config for a specific property if the config supplied by the user does not have it"
    (let [ziggurat-consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])
          consumer-config-without-commit-interval (dissoc ziggurat-consumer-config :commit-interval-ms)
          consumer-properties (#'ziggurat.kafka-consumer.consumer/build-consumer-properties-map ziggurat-consumer-config)]
      (with-redefs [ziggurat.kafka-consumer.consumer/build-consumer-properties-map (fn [consumer-config] (is (= (:commit-interval-ms consumer-config) (:commit-interval-ms ziggurat.kafka-consumer.consumer/default-consumer-config)))
                                                                                     consumer-properties)]
        (.close (create-consumer :consumer-1 consumer-config-without-commit-interval)))))
  (testing "returns nil when invalid configs are provided (KafkaConsumer throws Exception)"
    (let [consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])]
      (is (= nil (create-consumer :consumer-1 (assoc-in consumer-config [:consumer-group-id] nil)))))))
