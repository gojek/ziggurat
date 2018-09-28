(ns ziggurat.messaging.dead-set-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once fix/init-rabbit-mq)

(deftest replay-test
  (testing "puts message from dead set to instant queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"
            pushed-message    (doseq [_ (range count-of-messages)]
                                (producer/publish-to-dead-queue topic-name message))]
        (ds/replay count-of-messages topic-name nil)
        (doseq [_ (range count-of-messages)]
          (is (= message (rmq/get-msg-from-instant-queue topic-name))))
        (is (not (rmq/get-msg-from-instant-queue topic-name))))))

  (testing "puts message from dead set to instant channel queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"
            channel           "channel-1"
            pushed-message    (doseq [_ (range count-of-messages)]
                                (producer/publish-to-channel-dead-queue topic-name channel message))]
        (ds/replay count-of-messages topic-name channel)
        (doseq [_ (range count-of-messages)]
          (is (= message (rmq/get-message-from-channel-instant-queue topic-name channel))))
        (is (not (rmq/get-message-from-channel-instant-queue topic-name channel)))))))
