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
            topic-name        "default"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue topic-name message))
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
            channel           "channel-1"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-channel-dead-queue topic-name channel message))
        (ds/replay count-of-messages topic-name channel)
        (doseq [_ (range count-of-messages)]
          (is (= message (rmq/get-message-from-channel-instant-queue topic-name channel))))
        (is (not (rmq/get-message-from-channel-instant-queue topic-name channel)))))))

(deftest view-test
  (testing "gets dead set messages for topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"
            _                 (doseq [_ (range count-of-messages)]
                                (producer/publish-to-dead-queue topic-name message))
            dead-set-messages (ds/view count-of-messages topic-name nil)]
        (doseq [dead-set-message dead-set-messages]
          (is (= message dead-set-message)))
        (is (not (empty? (rmq/get-msg-from-dead-queue topic-name)))))))

  (testing "gets dead set messages for channel"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"
            channel           "channel-1"
            _                 (doseq [_ (range count-of-messages)]
                                (producer/publish-to-channel-dead-queue topic-name channel message))
            dead-set-messages (ds/view count-of-messages topic-name channel)]
        (doseq [dead-set-message dead-set-messages]
          (is (= message dead-set-message)))
        (is (not (empty? (rmq/get-msg-from-channel-dead-queue topic-name channel))))))))

(deftest delete-messages-test
  (testing "deletes messages from the dead set in order for a topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar" :number 0}
            topic-name        "default"
            _                 (doseq [index (range count-of-messages)]
                                (producer/publish-to-dead-queue topic-name (assoc message :number index)))
            _                 (ds/delete (- count-of-messages 1) topic-name nil)
            dead_set_messages (ds/view count-of-messages topic-name nil)]

        (is (= (count dead_set_messages) 1))
        (is (= dead_set_messages [(assoc message :number 9)])))))

  (testing "deletes all messages from the dead set for a topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue topic-name message))
        (ds/delete count-of-messages topic-name nil)
        (is (empty? (rmq/get-msg-from-dead-queue topic-name))))))

  (testing "deletes all messages from the dead set from a channel queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message           {:foo "bar"}
            topic-name        "default"
            channel-name      "channel-1"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-channel-dead-queue topic-name channel-name message))
        (ds/delete count-of-messages topic-name channel-name)
        (is (empty? (rmq/get-msg-from-channel-dead-queue topic-name channel-name)))))))
