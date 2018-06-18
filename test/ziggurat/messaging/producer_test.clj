(ns ziggurat.messaging.producer-test
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [ziggurat.config :refer [rabbitmq-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :as util]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once fix/init-rabbit-mq)

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-clear-data
      (let [message          {:foo "bar" :retry-count 5}
            expected-message {:foo "bar" :retry-count 4}
            topic-entity     :booking]
        (producer/retry message topic-entity)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "booking")]
          (is (= expected-message message-from-mq))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-clear-data
      (let [message          {:foo "bar" :retry-count 0}
            expected-message (dissoc message :retry-count)
            topic-entity     :booking]
        (producer/retry message topic-entity)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "booking")]
          (is (= expected-message message-from-mq))))))

  (testing "message with no retry count will publish to delay queue"
    (fix/with-clear-data
      (let [message           {:foo "bar"}
            expected-message  {:foo "bar" :retry-count 5}
            topic-entity      :booking]
        (producer/retry message topic-entity)
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
          stream-routes {:test {:handler-fn #(constantly nil)} :test2 {:handler-fn #(constantly nil)}}]
      (with-redefs [producer/create-and-bind-queue (fn
                                                     ([_ _] (swap! counter inc))
                                                     ([_ _ _ _] (swap! counter inc)))]
        (producer/make-queues stream-routes)
        (is (= (* (count stream-routes) 3) @counter)))))

  (testing "it creates queues with route identifier from stream routes"
    (with-open [ch (lch/open connection)]
      (let [stream-routes {:default {:handler-fn #(constantly :success)}}
            instant-queue-name (util/get-value-with-prefix-topic "default" (:queue-name (:instant (rabbitmq-config))))
            delay-queue-timeout (:queue-timeout-ms (:delay (rabbitmq-config)))
            delay-queue-name (util/get-value-with-prefix-topic "default" (format "%s_%s" (:queue-name (:delay (rabbitmq-config))) delay-queue-timeout))
            dead-queue-name (util/get-value-with-prefix-topic "default" (:queue-name (:dead-letter (rabbitmq-config))))
            instant-exchange-name (util/get-value-with-prefix-topic "default" (:exchange-name (:instant (rabbitmq-config))))
            delay-exchange-name (util/get-value-with-prefix-topic "default" (:exchange-name (:delay (rabbitmq-config))))
            dead-exchange-name (util/get-value-with-prefix-topic "default" (:exchange-name (:dead-letter (rabbitmq-config))))
            expected-queue-status {:message-count 0, :consumer-count 0}]
          (producer/make-queues stream-routes)
          (is (= (expected-queue-status (lq/status ch instant-queue-name))))
          (is (= (expected-queue-status (lq/status ch delay-queue-name))))
          (is (= (expected-queue-status (lq/status ch dead-queue-name))))
          (lq/delete ch instant-queue-name)
          (lq/delete ch delay-queue-name)
          (lq/delete ch delay-exchange-name)
          (lq/delete ch instant-exchange-name)
          (lq/delete ch dead-exchange-name)
          (lq/delete ch dead-queue-name)))))
