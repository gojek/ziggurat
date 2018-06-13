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
  (testing "it does not create queues when stream-routes are not passed"
    (let [counter (atom 0)]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([_ _] (swap! counter inc))
                                                     ([_ _ _ _] (swap! counter inc)))]
        (producer/make-queues nil)
        (producer/make-queues [])
        (is (= 0 @counter)))))

  (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
    (let [counter (atom 0)
          stream-routes [{:test {:handler-fn #(constantly nil)}} {:test2 {:handler-fn #(constantly nil)}}]]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([_ _] (swap! counter inc))
                                                     ([_ _ _ _] (swap! counter inc)))]
        (producer/make-queues stream-routes)
        (is (= (* (count stream-routes) 3) @counter)))))

  (testing "creates queues with route identifier from stream routes"
    (let [counter (atom 0)
          created-instant-queue (atom 0)
          created-delay-queue (atom 0)
          created-dead-queue (atom 0)
          stream-routes [{:default {:handler-fn #(constantly nil)}}]
          instant-queue-name "default_lambda_service_instant_queue_test"
          delay-queue-name "default_lambda_service_delay_queue_test_100"
          dead-queue-name "default_lambda_service_dead_letter_queue_test"]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([queue-name exchange-name]
                                                      (cond
                                                        (= queue-name instant-queue-name)
                                                        (swap! created-instant-queue inc)
                                                        (= queue-name dead-queue-name)
                                                        (swap! created-dead-queue inc))
                                                      (swap! counter inc))
                                                     ([queue-name exchange-name dead-letter-exchange-name _]
                                                      (cond
                                                        (= queue-name delay-queue-name)
                                                        (swap! created-delay-queue inc))
                                                      (swap! counter inc)))]
        (producer/make-queues stream-routes)
        (is (= (* (count stream-routes) 3) @counter))
        (is (= 1 @created-instant-queue))
        (is (= 1 @created-delay-queue))
        (is (= 1 @created-dead-queue))))))
