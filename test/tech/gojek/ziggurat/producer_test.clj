(ns tech.gojek.ziggurat.producer-test
  (:require [clojure.test :refer :all]
            [clojure.string :refer [blank?]]
            [clojure.test.check.generators :as gen])
  (:import (tech.gojek.ziggurat Producer)
           (org.apache.kafka.streams.integration.utils IntegrationTestUtils)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (java.util Properties)
           (tech.gojek.ziggurat.test Fixtures)))

(defn get-common-consumer-properties []
  (doto (new Properties)
    (.setProperty ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092")
    (.setProperty ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest")
    (.setProperty ConsumerConfig/GROUP_ID_CONFIG "ziggurat-java-test-config-id")))

(defn getConsumerConfigForStringTypeKeyAndByteArrayTypeValue []
  (doto (get-common-consumer-properties)
    (.setProperty ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer")
    (.setProperty ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArrayDeserializer")))

(defn getConsumerConfigForKeyAndValueOfTypeString []
  (doto (get-common-consumer-properties)
    (.setProperty ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer")
    (.setProperty ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer")))

(defn getConsumerConfigForKeyAndValueOfTypeByteArray []
  (doto (get-common-consumer-properties)
    (.setProperty ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    (.setProperty ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.ByteArrayDeserializer")))

(defn mount-config-and-producer-the-java-way [f]
  (Fixtures/mountConfig)
  (Fixtures/mountProducer)
  (f)
  (Fixtures/unmountAll))

(use-fixtures :once mount-config-and-producer-the-java-way)

(deftest should-send-string-data
  (testing "producer should be able to send key value pairs of type String"
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          kafka-topic  (gen/generate alphanum-gen 10)
          key "Key"
          value "Sent from Java"]
      (Producer/send :default kafka-topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived
                    (getConsumerConfigForKeyAndValueOfTypeString) kafka-topic 1 2000)]
        (is (= key (.key (.get result 0))))
        (is (= value (.value (.get result 0))))))))

;; This test should have failed because the test is trying to consume
;; a byte array data using StringDeserializer class.
(deftest should-send-byte-array-data
  (testing "producer should be able to send key value pairs of type Byte Array"
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          kafka-topic  (gen/generate alphanum-gen 10)
          key (.getBytes "Key")
          value (.getBytes "Sent from Java")]
      (Producer/send :with-byte-array-producer kafka-topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived
                    (getConsumerConfigForKeyAndValueOfTypeString) kafka-topic 1 2000)]
        (is (= "Key" (String. (.key (.get result 0)))))
        (is (= "Sent from Java" (String. (.value (.get result 0)))))))))

(deftest should-send-byte-array-data
  (testing "producer should be able to send key value pairs of type Byte Array"
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          kafka-topic  (gen/generate alphanum-gen 10)
          key (.getBytes "Key")
          value (.getBytes "Sent from Java")]
      (Producer/send :with-byte-array-producer kafka-topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived
                    (getConsumerConfigForKeyAndValueOfTypeByteArray) kafka-topic 1 2000)]
        (is (= "Key" (String. (.key (.get result 0)))))
        (is (= "Sent from Java" (String. (.value (.get result 0)))))))))

(deftest should-send-data-with-different-types-for-key-and-value
  (testing "producer should be able to send different types of data for key and value"
    (let [alphanum-gen (gen/such-that #(not (blank? %)) gen/string-alphanumeric)
          kafka-topic  (gen/generate alphanum-gen 10)
          key "Key"
          value (.getBytes "Sent from Java")]
      (Producer/send :with-diff-key-val-type-producer kafka-topic key value)
      (let [result (IntegrationTestUtils/waitUntilMinKeyValueRecordsReceived
                    (getConsumerConfigForStringTypeKeyAndByteArrayTypeValue) kafka-topic 1 2000)]
        (is (= "Key" (.key (.get result 0))))
        (is (= "Sent from Java" (String. (.value (.get result 0)))))))))
