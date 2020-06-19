(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.util :refer [prefixed-queue-name is-connection-required?]]))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-entity and queue-name, it returns a snake case string with topic-entity as the prefix of queue-name"
    (let [topic-entity    :topic-name
          queue-name      "queue_name"
          expected-string "topic-name_queue_name"]
      (is (= (prefixed-queue-name topic-entity queue-name) expected-string)))))

(deftest is-connection-required?-test
  (testing "when retry is enabled and channels are not provided it should return true"
    (let [orig-ziggurat-config (ziggurat-config)
          stream-routes {:test-topic {:handler-fn (constantly nil)}}]
      (with-redefs [ziggurat.config/ziggurat-config (fn [] (assoc orig-ziggurat-config
                                                                  :retry {:enabled true}))]
        (is (true? (is-connection-required? (ziggurat.config/ziggurat-config) stream-routes))))))

  (testing "when retry is disabled and channels are not provided it should return false"
    (let [orig-ziggurat-config (ziggurat-config)
          stream-routes {:test-topic {:handler-fn (constantly nil)}}]
      (with-redefs [ziggurat.config/ziggurat-config (fn [] (assoc orig-ziggurat-config
                                                                  :retry {:enabled false}))]
        (is (false? (is-connection-required? (ziggurat.config/ziggurat-config) stream-routes))))))

  (testing "when retry is disabled and channels are not provided in stream routes it should return false"
    (let [orig-ziggurat-config (ziggurat-config)
          stream-router-config {:test-topic {:channels {:test-channel {:worker-count 10}}}}
          stream-routes {:test-topic {:handler-fn (constantly nil)}}]
      (with-redefs [ziggurat.config/ziggurat-config (fn [] (assoc orig-ziggurat-config
                                                                  :stream-router stream-router-config
                                                                  :retry {:enabled false}))]
        (is (false? (is-connection-required? (ziggurat.config/ziggurat-config) stream-routes))))))

  (testing "when retry is disabled and channels are provided in stream routes it should return true"
    (let [orig-ziggurat-config (ziggurat-config)
          stream-router-config {:test-topic {:channels {:test-channel {:worker-count 10}}}}
          stream-routes        {:test-topic {:handler-fn   (constantly nil)
                                             :test-channel (constantly nil)}}]
      (with-redefs [ziggurat.config/ziggurat-config (fn [] (assoc orig-ziggurat-config
                                                                  :stream-router stream-router-config
                                                                  :retry {:enabled false}))]
        (is (true? (is-connection-required? (ziggurat.config/ziggurat-config) stream-routes))))))

  (testing "when retry is enabled and channels are provided in stream routes it should return true"
    (let [orig-ziggurat-config (ziggurat-config)
          stream-router-config {:test-topic {:channels {:test-channel {:worker-count 10}}}}
          stream-routes        {:test-topic {:handler-fn   (constantly nil)
                                             :test-channel (constantly nil)}}]
      (with-redefs [ziggurat.config/ziggurat-config (fn [] (assoc orig-ziggurat-config
                                                                  :stream-router stream-router-config
                                                                  :retry {:enabled true}))]
        (is (true? (is-connection-required? (ziggurat.config/ziggurat-config) stream-routes)))))))