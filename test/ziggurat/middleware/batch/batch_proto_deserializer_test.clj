(ns ziggurat.middleware.batch.batch-proto-deserializer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.middleware.batch.batch-proto-deserializer :refer :all]
            [protobuf.core :as proto])
  (:import (flatland.protobuf.test Example$Photo Example$Photo$Tag)
           (com.gojek.test.proto PersonTestProto$Person)))

(deftest batch-proto-deserializer-test
  (let [key                          {:person-id 100}
        value                        {:id 7 :path "/photos/h2k3j4h9h23"}
        key-with-struct              {:id         100
                                      :characters {:fields
                                                   [{:key "hobbies",
                                                     :value
                                                          {:list-value {:values [{:string-value "eating"}
                                                                                 {:string-value "sleeping"}]}}}]}}
        flattened-key-with-struct    {:id         100
                                      :characters {:hobbies ["eating" "sleeping"]}}
        value-with-struct            {:id         100
                                      :name       "John"
                                      :characters {:fields
                                                   [{:key "physique",
                                                     :value
                                                          {:struct-value
                                                           {:fields
                                                            [{:key "height", :value {:number-value 180.12}}
                                                             {:key "weight", :value {:number-value 80.34}}]}}}
                                                    {:key "age", :value {:number-value 50.5}}
                                                    {:key "gender", :value {:string-value "male"}}]}}
        flattened-value-with-struct  {:id         100
                                      :name       "John"
                                      :characters {:physique {:height 180.12 :weight 80.34}
                                                   :age      50.5
                                                   :gender   "male"}}
        key-proto-class              Example$Photo$Tag
        value-proto-class            Example$Photo
        struct-proto-class           PersonTestProto$Person
        serialized-key               (proto/->bytes (proto/create key-proto-class key))
        serialized-value             (proto/->bytes (proto/create value-proto-class value))
        serialized-key-with-struct   (proto/->bytes (proto/create struct-proto-class key-with-struct))
        serialized-value-with-struct (proto/->bytes (proto/create struct-proto-class value-with-struct))]
    (testing "deserializes key and value for all the messages in a batch"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key :value serialized-value})
            handler-fn         (fn [batch]
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
        (is (true? @handler-fn-called?))))
    (testing "Does not deserialize nil key or value"
      (let [handler-fn-called-2? (atom false)
            batch-message        [{:key nil :value serialized-value} {:key serialized-key :value nil}]
            handler-fn           (fn [batch]
                                   (is (seq? batch))
                                   (loop [msg batch]
                                     (when (not-empty msg)
                                       (let [key-value-pair     (first msg)
                                             deserialized-key   (:key key-value-pair)
                                             deserialized-value (:value key-value-pair)]
                                         (is (map? key-value-pair))
                                         (is (or (nil? deserialized-key) (= key deserialized-key)))
                                         (is (or (nil? deserialized-value) (= value deserialized-value)))
                                         (recur (rest msg)))))
                                   (reset! handler-fn-called-2? true))]
        ((deserialize-batch-of-proto-messages handler-fn key-proto-class value-proto-class :some-topic-entity) batch-message)
        (is (true? @handler-fn-called-2?))))
    (testing "should not deserializes key and value for the messages in a batch and flatten protobuf structs if flatten-protobuf-struct? is false"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key-with-struct :value serialized-value-with-struct})
            handler-fn         (fn [batch]
                                 (is (seq? batch))
                                 (is (= 5 (count batch)))
                                 (loop [msg batch]
                                   (when (not-empty msg)
                                     (let [key-value-pair (first msg)]
                                       (is (map? key-value-pair))
                                       (is (= key-with-struct (:key key-value-pair)))
                                       (is (= value-with-struct (:value key-value-pair)))
                                       (recur (rest msg)))))
                                 (reset! handler-fn-called? true))]
        ((deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity false) batch-message)
        (is (true? @handler-fn-called?))))
    (testing "should deserializes key and value for the messages in a batch and flatten protobuf structs if flatten-protobuf-struct? is true"
      (let [handler-fn-called? (atom false)
            batch-message      (repeat 5 {:key serialized-key-with-struct :value serialized-value-with-struct})
            handler-fn         (fn [batch]
                                 (is (seq? batch))
                                 (is (= 5 (count batch)))
                                 (loop [msg batch]
                                   (when (not-empty msg)
                                     (let [key-value-pair (first msg)]
                                       (is (map? key-value-pair))
                                       (is (= flattened-key-with-struct (:key key-value-pair)))
                                       (is (= flattened-value-with-struct (:value key-value-pair)))
                                       (recur (rest msg)))))
                                 (reset! handler-fn-called? true))]
        ((deserialize-batch-of-proto-messages handler-fn struct-proto-class struct-proto-class :some-topic-entity true) batch-message)
        (is (true? @handler-fn-called?))))))
