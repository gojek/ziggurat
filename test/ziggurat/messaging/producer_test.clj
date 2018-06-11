(ns ziggurat.messaging.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once fix/init-rabbit-mq)

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-clear-data
      (let [message {:foo "bar" :retry-count 5}
            expected-message {:foo "bar" :retry-count 4}
            topic "booking"]
        (producer/retry message topic)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "booking")]
          (is (= expected-message message-from-mq))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-clear-data
      (let [message {:foo "bar" :retry-count 0}
            expected-message (dissoc message :retry-count)
            topic "booking"]
        (producer/retry message topic)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "booking")]
          (is (= expected-message message-from-mq))))))

  (testing "message with no retry count will publish to delay queue"
    (fix/with-clear-data
      (let [message {:foo "bar"}
            expected-message {:foo "bar" :retry-count 5}
            topic "booking"]
        (producer/retry message topic)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "booking")]
          (is (= message-from-mq expected-message)))))))

(deftest make-queues-test
  (testing "it calls create-and-bind-queue for each queue creation when a mapper-fn is passed"
    (let [counter (atom 0)]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([_ _] (swap! counter inc))
                                                     ([_ _ _ _] (swap! counter inc)))]
        (producer/make-queues nil)
        (is (= 3 @counter)))))
  (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
    (let [counter (atom 0)
          stream-routes [{:test {:handler-fn #(constantly nil)}} {:test2 {:handler-fn #(constantly nil)}}]]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([_ _] (swap! counter inc))
                                                     ([_ _ _ _] (swap! counter inc)))]
        (producer/make-queues stream-routes)
        (is (= (* (count stream-routes) 3) @counter))))))