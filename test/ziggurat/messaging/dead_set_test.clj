(ns ziggurat.messaging.dead-set-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once (join-fixtures [fix/init-messaging
                                    fix/silence-logging
                                    fix/mount-metrics]))

(def topic-entity :default)
(def default-message-payload {:message {:foo "bar"} :topic-entity topic-entity :retry-count 0})

(deftest replay-test
  (testing "puts message from dead set to instant queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue message-payload))
        (ds/replay count-of-messages topic-entity nil)
        (doseq [_ (range count-of-messages)]
          (is (= message-payload (rmq/get-msg-from-instant-queue (name topic-entity)))))
        (is (not (rmq/get-msg-from-instant-queue (name topic-entity)))))))

  (testing "puts message from dead set to instant channel queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload
            channel           "channel-1"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-channel-dead-queue channel message-payload))
        (ds/replay count-of-messages topic-entity channel)
        (doseq [_ (range count-of-messages)]
          (is (= message-payload (rmq/get-message-from-channel-instant-queue (name topic-entity) channel))))
        (is (not (rmq/get-message-from-channel-instant-queue topic-entity channel)))))))

(deftest view-test
  (testing "gets dead set messages for topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload
            _                 (doseq [_ (range count-of-messages)]
                                (producer/publish-to-dead-queue message-payload))
            dead-set-messages (ds/view count-of-messages topic-entity nil)]
        (doseq [dead-set-message dead-set-messages]
          (is (= message-payload dead-set-message)))
        (is (not (empty? (rmq/get-msg-from-dead-queue (name topic-entity))))))))

  (testing "gets dead set messages for channel"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload
            channel           "channel-1"
            _                 (doseq [_ (range count-of-messages)]
                                (producer/publish-to-channel-dead-queue channel message-payload))
            dead-set-messages (ds/view count-of-messages topic-entity channel)]
        (doseq [dead-set-message dead-set-messages]
          (is (= message-payload dead-set-message)))
        (is (not (empty? (rmq/get-msg-from-channel-dead-queue (name topic-entity) channel))))))))

(deftest delete-messages-test
  (testing "deletes messages from the dead set in order for a topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages       10
            message-payload         (assoc-in default-message-payload [:message :number] 0)
            message-payloads        (map #(assoc-in message-payload [:message :number] %) (range count-of-messages))
            topic-entity              "default"
            remaining-message-count 2
            delete-count            (- count-of-messages remaining-message-count)
            _                       (doseq [message message-payloads]
                                      (producer/publish-to-dead-queue message))
            _                       (ds/delete delete-count topic-entity nil)
            dead-set-messages       (ds/view count-of-messages topic-entity nil)]

        (is (= (count dead-set-messages) remaining-message-count))
        (is (= dead-set-messages (take-last remaining-message-count message-payloads))))))

  (testing "deletes all messages from the dead set for a topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue message-payload))
        (ds/delete count-of-messages topic-entity nil)
        (is (empty? (rmq/get-msg-from-dead-queue (name topic-entity)))))))

  (testing "deletes all messages from the dead set from a channel queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload
            channel-name      "channel-1"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-channel-dead-queue channel-name message-payload))
        (ds/delete count-of-messages topic-entity channel-name)
        (is (empty? (rmq/get-msg-from-channel-dead-queue (name topic-entity) channel-name)))))))
