(ns ziggurat.middleware.batch.batch-proto-deserializer-test
  (:require [clojure.test :refer [deftest is testing]])
  (:require [protobuf.core :as proto]
            [ziggurat.middleware.batch.batch-proto-deserializer :as bpd])
  (:import (com.gojek.test.proto PersonTestProto$Person)))

(deftest batch-proto-deserializer-test
  (let [key                         {:id 100}
        flattened-key-with-struct   {:id 100}
        value                       {:id   100
                                     :name "John"}
        flattened-value-with-struct {:id   100
                                     :name "John"}
        struct-proto-class          PersonTestProto$Person
        serialized-key              (proto/->bytes (proto/create struct-proto-class key))
        serialized-value            (proto/->bytes (proto/create struct-proto-class value))]
    (testing "deserializes key and value for all the messages in a batch"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key :value serialized-value})
            handler-fn         (fn [batch]
                                 (is (seq? batch))
                                 (loop [msg batch]
                                   (when (not-empty msg)
                                     (let [key-value-pair (first msg)]
                                       (is (map? key-value-pair))
                                       (is (= {:message key :topic-entity :some-topic-entity :retry-count 0} (:key key-value-pair)))
                                       (is (= {:message value :topic-entity :some-topic-entity :retry-count 0} (:value key-value-pair)))
                                       (recur (rest msg)))))
                                 (reset! handler-fn-called? true))]
        ((bpd/deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity) batch-message)
        (is (true? @handler-fn-called?))))
    (testing "Does not deserialize nil key or value"
      (let [handler-fn-called-2? (atom false)
            batch-message        (repeat 5 {:key serialized-key :value serialized-value}) ;; how is this nil?
            handler-fn           (fn [batch]
                                   (is (seq? batch))
                                   (loop [msg batch]
                                     (when (not-empty msg)
                                       (let [key-value-pair     (first msg)
                                             deserialized-key   (:key key-value-pair)
                                             deserialized-value (:value key-value-pair)]
                                         (is (map? key-value-pair))
                                         (is (= {:message key :topic-entity :some-topic-entity :retry-count 0}  deserialized-key))
                                         (is (= {:message value :topic-entity :some-topic-entity :retry-count 0} deserialized-value))
                                         (recur (rest msg)))))
                                   (reset! handler-fn-called-2? true))]
        ((bpd/deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity) batch-message)
        (is (true? @handler-fn-called-2?))))
    (testing "should not deserializes key and value for the messages in a batch and flatten protobuf structs if flatten-protobuf-struct? is false"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key :value serialized-value})
            handler-fn         (fn [batch]
                                 (is (seq? batch))
                                 (is (= 5 (count batch)))
                                 (loop [msg batch]
                                   (when (not-empty msg)
                                     (let [key-value-pair (first msg)]
                                       (is (map? key-value-pair))
                                       (is (= {:message key :topic-entity :some-topic-entity :retry-count 0} (:key key-value-pair)))
                                       (is (= {:message value :topic-entity :some-topic-entity :retry-count 0} (:value key-value-pair)))
                                       (recur (rest msg)))))
                                 (reset! handler-fn-called? true))]
        ((bpd/deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity false) batch-message)
        (is (true? @handler-fn-called?))))
    (testing "should deserializes key and value for the messages in a batch and flatten protobuf structs if flatten-protobuf-struct? is true"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key :value serialized-value})
            handler-fn         (fn [batch]
                                 (is (seq? batch))
                                 (is (= 5 (count batch)))
                                 (loop [msg batch]
                                   (when (not-empty msg)
                                     (let [key-value-pair (first msg)]
                                       (is (map? key-value-pair))
                                       (is (= {:message flattened-key-with-struct :topic-entity :some-topic-entity :retry-count 0} (:key key-value-pair)))
                                       (is (= {:message flattened-value-with-struct :topic-entity :some-topic-entity :retry-count 0} (:value key-value-pair)))
                                       (recur (rest msg)))))
                                 (reset! handler-fn-called? true))]
        ((bpd/deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity true) batch-message)
        (is (true? @handler-fn-called?))))))
