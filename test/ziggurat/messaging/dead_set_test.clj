(ns ziggurat.messaging.dead-set-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.dead-set :refer :all]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))

(defn- gen-message-payload [topic-entity]
  {:message {:gen-key (apply str (take 10 (repeatedly #(char (+ (rand 26) 65)))))}
   :topic-entity topic-entity})

(use-fixtures :once fix/init-rabbit-mq)

(def topic-entity :default)
(def default-message-payload {:message {:foo "bar"} :topic-entity topic-entity :retry-count 0})

(deftest get-dead-set-messages-for-topic-test
  (let [message-payload   (assoc (gen-message-payload topic-entity) :retry-count 0)]
    (testing "when ack is enabled, get the dead set messages and remove from dead set"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)}}
        (let [count-of-messages 10
              pushed-message    (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-dead-queue message-payload))
              dead-set-messages (get-dead-set-messages-for-topic true topic-entity count-of-messages)]
          (is (= (repeat count-of-messages message-payload) dead-set-messages))
          (is (empty? (get-dead-set-messages-for-topic true topic-entity count-of-messages))))))

    (testing "when ack is disabled, get the dead set messages and not remove from dead set"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)}}
        (let [count-of-messages 10
              pushed-message    (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-dead-queue message-payload))
              dead-set-messages (get-dead-set-messages-for-topic false topic-entity count-of-messages)]
          (is (= (repeat count-of-messages message-payload) dead-set-messages))
          (is (= (repeat count-of-messages message-payload) (get-dead-set-messages-for-topic false topic-entity count-of-messages))))))))

(deftest get-dead-set-messages-from-channel-test
  (let [message-payload (assoc (gen-message-payload topic-entity) :retry-count 0)]
    (testing "when ack is enabled, get the dead set messages and remove from dead set"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)
                                      :channel-1  (constantly nil)}}
        (let [count-of-messages 10
              channel           "channel-1"
              pushed-message    (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-channel-dead-queue channel message-payload))
              dead-set-messages (get-dead-set-messages-for-channel true topic-entity channel count-of-messages)]
          (is (= (repeat count-of-messages message-payload) dead-set-messages))
          (is (empty? (get-dead-set-messages-for-channel true topic-entity channel count-of-messages))))))

    (testing "when ack is disabled, get the dead set messages and not remove from dead set"
      (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
        (let [count-of-messages 10
              pushed-message    (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-dead-queue message-payload))
              dead-set-messages (get-dead-set-messages-for-topic false topic-entity count-of-messages)]
          (is (= (repeat count-of-messages message-payload) dead-set-messages))
          (is (= (repeat count-of-messages message-payload) (get-dead-set-messages-for-topic false topic-entity count-of-messages))))))))

(deftest replay-test
  (testing "puts message from dead set to instant queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue message-payload))
        (replay count-of-messages topic-entity nil)
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
        (replay count-of-messages topic-entity channel)
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
            dead-set-messages (view count-of-messages topic-entity nil)]
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
            dead-set-messages (view count-of-messages topic-entity channel)]
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
            _                       (delete delete-count topic-entity nil)
            dead-set-messages       (view count-of-messages topic-entity nil)]

        (is (= (count dead-set-messages) remaining-message-count))
        (is (= dead-set-messages (take-last remaining-message-count message-payloads))))))

  (testing "deletes all messages from the dead set for a topic"
    (fix/with-queues {:default {:handler-fn (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-dead-queue message-payload))
        (delete count-of-messages topic-entity nil)
        (is (empty? (rmq/get-msg-from-dead-queue (name topic-entity)))))))

  (testing "deletes all messages from the dead set from a channel queue"
    (fix/with-queues {:default {:handler-fn (constantly nil)
                                :channel-1  (constantly nil)}}
      (let [count-of-messages 10
            message-payload   default-message-payload
            channel-name      "channel-1"]
        (doseq [_ (range count-of-messages)]
          (producer/publish-to-channel-dead-queue channel-name message-payload))
        (delete count-of-messages topic-entity channel-name)
        (is (empty? (rmq/get-msg-from-channel-dead-queue (name topic-entity) channel-name)))))))
