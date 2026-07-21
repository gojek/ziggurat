(ns ziggurat.kafka-consumer.consumer-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]])
  (:require [clonfig.core :as clonfig]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.kafka-consumer.consumer :refer [create-consumer]])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer)))

(use-fixtures :once fix/mount-only-config)

(deftest create-consumer-test
  (testing "create the consumer with provided config and subscribe to provided topic"
    (let [consumer              ^KafkaConsumer (create-consumer :consumer-1 (get-in (ziggurat-config) [:batch-routes :consumer-1]))
          expected-origin-topic (get-in (ziggurat-config) [:batch-routes :consumer-1 :origin-topic])]
      (is (contains? (set (keys (.listTopics consumer))) expected-origin-topic))
      (.unsubscribe consumer)
      (.close consumer)))

  (testing "returns nil when invalid configs are provided (KafkaConsumer throws Exception)"
    (let [consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])]
      (is (= nil (create-consumer :consumer-1 (assoc-in consumer-config [:consumer-group-id] nil)))))))

(deftest with-manual-commit-config-test
  (let [with-manual-commit-config #'ziggurat.kafka-consumer.consumer/with-manual-commit-config]
    (testing "disables kafka auto-commit when manual-commit-enabled is true"
      (is (false? (:enable-auto-commit (with-manual-commit-config {:manual-commit-enabled true})))))
    (testing "leaves auto-commit untouched when manual-commit-enabled is absent or false"
      (is (nil? (:enable-auto-commit (with-manual-commit-config {}))))
      (is (nil? (:enable-auto-commit (with-manual-commit-config {:manual-commit-enabled false})))))))

(def ^:private cooperative-sticky-class "org.apache.kafka.clients.consumer.CooperativeStickyAssignor")
(def ^:private range-class "org.apache.kafka.clients.consumer.RangeAssignor")

(deftest resolve-partition-assignment-strategy-test
  (let [resolve-strategy #'ziggurat.kafka-consumer.consumer/resolve-partition-assignment-strategy]
    (testing "resolves a known short name to its fully-qualified assignor class"
      (is (= cooperative-sticky-class (resolve-strategy :cooperative-sticky)))
      (is (= range-class (resolve-strategy :range)))
      (is (= "org.apache.kafka.clients.consumer.RoundRobinAssignor" (resolve-strategy :round-robin)))
      (is (= "org.apache.kafka.clients.consumer.StickyAssignor" (resolve-strategy :sticky))))
    (testing "accepts a short name given as a string"
      (is (= cooperative-sticky-class (resolve-strategy "cooperative-sticky"))))
    (testing "passes a fully-qualified class name through unchanged (forward compatible)"
      (is (= "com.example.CustomAssignor" (resolve-strategy "com.example.CustomAssignor"))))
    (testing "joins an ordered collection into a Kafka preference list (two-phase migration)"
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy [:range :cooperative-sticky])))
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy ["range" "cooperative-sticky"]))))
    (testing "resolves a collection that mixes short names and fully-qualified names"
      (is (= (str "com.example.CustomAssignor," cooperative-sticky-class)
             (resolve-strategy ["com.example.CustomAssignor" :cooperative-sticky]))))
    ;; A comma separated string is the only preference-list form usable in config.edn,
    ;; because clonfig destructures vectors as [default-value post-processor].
    (testing "splits a comma separated string into a preference list"
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy "range,cooperative-sticky"))))
    (testing "tolerates whitespace around comma separated entries"
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy "range , cooperative-sticky")))
      (is (= cooperative-sticky-class (resolve-strategy "  cooperative-sticky  "))))
    (testing "passes a comma separated list of fully-qualified names through unchanged"
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy (str range-class "," cooperative-sticky-class)))))
    (testing "ignores empty segments produced by a trailing or doubled comma"
      (is (= range-class (resolve-strategy "range,")))
      (is (= (str range-class "," cooperative-sticky-class)
             (resolve-strategy "range,,cooperative-sticky"))))
    (testing "throws on an unrecognised short name so misconfiguration fails loudly"
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy :does-not-exist)))
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy "not-a-known-strategy"))))
    (testing "throws when a strategy inside a collection is unrecognised"
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy [:cooperative-sticky :nope]))))
    (testing "throws when a strategy inside a comma separated string is unrecognised"
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy "range,nope"))))
    (testing "throws on empty, blank and comma-only input"
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy [])))
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy "")))
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy "   ")))
      (is (thrown? clojure.lang.ExceptionInfo (resolve-strategy ","))))))

(deftest partition-assignment-strategy-config-edn-compatibility-test
  ;; Regression guard for the documented config format. Ziggurat reads config.edn through
  ;; clonfig, which destructures EVERY vector as a [default-value post-processor] pair and
  ;; throws on an unknown post-processor. A preference list must therefore be written as a
  ;; comma separated string; if the docs ever regress to a vector this test fails.
  (let [read-strategy (fn [v]
                        (get-in (clonfig/read-config
                                 {:batch-routes {:consumer-1 {:partition-assignment-strategy v}}})
                                [:batch-routes :consumer-1 :partition-assignment-strategy]))]
    (testing "a comma separated string survives config reading intact"
      (is (= "range,cooperative-sticky" (read-strategy "range,cooperative-sticky"))))
    (testing "a single keyword and a single string survive config reading intact"
      (is (= :cooperative-sticky (read-strategy :cooperative-sticky)))
      (is (= "cooperative-sticky" (read-strategy "cooperative-sticky"))))
    (testing "a vector cannot be used for a preference list - clonfig rejects it"
      (is (thrown? RuntimeException (read-strategy [:range :cooperative-sticky])))
      (is (thrown? RuntimeException (read-strategy ["range" "cooperative-sticky"]))))))

(deftest with-partition-assignment-strategy-config-test
  (let [with-strategy #'ziggurat.kafka-consumer.consumer/with-partition-assignment-strategy-config]
    (testing "resolves a single strategy into the kafka property key"
      (is (= cooperative-sticky-class
             (:partition-assignment-strategy-config
              (with-strategy {:partition-assignment-strategy :cooperative-sticky})))))
    (testing "resolves an ordered collection into a preference list"
      (is (= (str range-class "," cooperative-sticky-class)
             (:partition-assignment-strategy-config
              (with-strategy {:partition-assignment-strategy [:range :cooperative-sticky]})))))
    (testing "preserves the rest of the config untouched"
      (is (= "topic" (:origin-topic
                      (with-strategy {:origin-topic "topic" :partition-assignment-strategy :range})))))
    (testing "leaves config unchanged when the flag is absent or nil (default eager behaviour)"
      (is (nil? (:partition-assignment-strategy-config (with-strategy {}))))
      (is (nil? (:partition-assignment-strategy-config
                 (with-strategy {:partition-assignment-strategy nil})))))))

(deftest create-consumer-partition-assignment-strategy-test
  (let [consumer-config (get-in (ziggurat-config) [:batch-routes :consumer-1])]
    (testing "builds a consumer when a valid partition-assignment-strategy is configured"
      (let [consumer ^KafkaConsumer (create-consumer :consumer-1 consumer-config)]
        (is (some? consumer))
        (.unsubscribe consumer)
        (.close consumer)))
    (testing "returns nil when an invalid partition-assignment-strategy is configured (fails loudly, caught)"
      (is (nil? (create-consumer :consumer-1
                                 (assoc consumer-config :partition-assignment-strategy :not-a-real-strategy)))))))
