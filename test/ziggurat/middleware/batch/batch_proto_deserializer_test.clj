(ns ziggurat.middleware.batch.batch-proto-deserializer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.middleware.batch.batch-proto-deserializer :refer :all]
            [protobuf.core :as proto])
  (:import (flatland.protobuf.test Example$Photo Example$Photo$Tag)))

(deftest batch-proto-deserializer-test
  (testing "deserializes key and value for all the messages in a batch"
    (let [key                    {:person-id 100}
          value                  {:id 7 :path "/photos/h2k3j4h9h23"}
          key-proto-class        Example$Photo$Tag
          value-proto-class      Example$Photo
          serialized-key         (proto/->bytes (proto/create key-proto-class key))
          serialized-value       (proto/->bytes (proto/create value-proto-class value))
          batch-message          (repeat 5 {:key serialized-key :value serialized-value})
          handler-fn-called?     (atom false)
          handler-fn             (fn [batch]
                                   (is (seq? batch))
                                   (loop [msg batch]
                                     (when (not-empty msg)
                                       (let [key-value-pair (first msg)]
                                         (is (map? key-value-pair))
                                         (is (= key (:key key-value-pair)))
                                         (is (= value (:value key-value-pair)))
                                         (recur (rest msg)))))
                                   (reset! handler-fn-called? true))]
      ((deserialize-batch-of-proto-messages handler-fn key-proto-class value-proto-class :some-topic-entity) batch-message)
      (is (true? @handler-fn-called?)))))

(deftest batch-proto-deserializer-test-with-nil-values
  (testing "Does not deserialize nil key or value"
    (let [key                    {:person-id 100}
          value                  {:id 7 :path "/photos/h2k3j4h9h23"}
          key-proto-class        Example$Photo$Tag
          value-proto-class      Example$Photo
          serialized-key         (proto/->bytes (proto/create key-proto-class key))
          serialized-value       (proto/->bytes (proto/create value-proto-class value))
          batch-message          [{:key nil :value serialized-value} {:key serialized-key :value nil}]
          handler-fn-called?     (atom false)
          handler-fn             (fn [batch]
                                   (is (seq? batch))
                                   (loop [msg batch]
                                     (when (not-empty msg)
                                       (let [key-value-pair (first msg)
                                             deserialized-key (:key key-value-pair)
                                             deserialized-value (:value key-value-pair)]
                                         (is (map? key-value-pair))
                                         (is (or (nil? deserialized-key) (= key deserialized-key)))
                                         (is (or (nil? deserialized-value) (= value deserialized-value)))
                                         (recur (rest msg)))))
                                   (reset! handler-fn-called? true))]
      ((deserialize-batch-of-proto-messages handler-fn key-proto-class value-proto-class :some-topic-entity) batch-message)
      (is (true? @handler-fn-called?)))))
