(ns ziggurat.messaging.rabbitmq-wrapper-consumer-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.consumer :refer :all]
            [langohr.channel :as lch]
            [ziggurat.util.rabbitmq :as util]
            [langohr.basic :as lb]
            [ziggurat.mapper :as mpr]
            [taoensso.nippy :as nippy]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging
                                    fix/mount-metrics]))

(defn- gen-message-payload [topic-entity]
  {:message {:gen-key (apply str (take 10 (repeatedly #(char (+ (rand 26) 65)))))}
   :topic-entity topic-entity})

(def topic-entity :default)

(deftest process-message-test
  (testing "process-message function should ack message after once processing finishes"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity)
            processing-fn (fn [message-arg]
                            (is (= message-arg message)))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open rmqw/connection)]
          (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                prefixed-queue-name (str topic-entity-name "_" queue-name)
                [meta payload]      (lb/get ch prefixed-queue-name false)
                _                   (rmqw/process-message-from-queue ch meta payload processing-fn)
                consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
            (is (= consumed-message nil)))))))
  (testing "process-message function not process a message if convert-message returns nil"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity)
            processing-fn-called (atom false)
            processing-fn (fn [message-arg]
                            (if (nil? message-arg)
                              (reset! processing-fn-called true)))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-redefs [read-messages-from-queue (fn [_ _ _ _] nil)]
          (with-open [ch (lch/open rmqw/connection)]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload]      (lb/get ch prefixed-queue-name false)
                  _                   (rmqw/process-message-from-queue ch meta payload processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= false @processing-fn-called))
              (is (= consumed-message nil))))))))
  (testing "process-message function should reject and re-queue a message if processing fails"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity)
            processing-fn (fn [message-arg]
                            (is (= message-arg message))
                            (throw (Exception. "exception message")))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open rmqw/connection)]
          (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                prefixed-queue-name (str topic-entity-name "_" queue-name)
                [meta payload]      (lb/get ch prefixed-queue-name false)
                _                   (rmqw/process-message-from-queue ch meta payload processing-fn)
                consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
            (is (= consumed-message message)))))))
  (testing "process-message function should reject and discard a message if message conversion fails"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity)
            processing-fn (fn [_] ())
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open rmqw/connection)]
          (with-redefs [ziggurat.messaging.consumer/convert-to-message-payload (fn [] (throw (Exception. "exception message")))]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload]      (lb/get ch prefixed-queue-name false)
                  _                   (rmqw/process-message-from-queue ch meta payload processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= consumed-message nil)))))))))

(deftest ack-and-convert-message
  (testing "While constructing a MessagePayload, adds topic-entity as a keyword and retry-count as 0 if message does not already has :retry-count"
    (let [message                   {:foo "bar"}
          expected-message-payload  (assoc (mpr/->MessagePayload (dissoc message :retry-count) topic-entity) :retry-count 0)
          consumed-message (rmqw/consume-message nil {:delivery-tag "delivery-tag"} (nippy/freeze message) false)
          converted-message-payload (convert-to-message-payload consumed-message "default")]
      (is (= converted-message-payload expected-message-payload))))
  (testing "While constructing a MessagePayload, adds topic-entity as a keyword and retry-count as it exists in the message"
    (let [message                   {:foo "bar" :retry-count 4}
          expected-message-payload  (assoc (mpr/->MessagePayload (dissoc message :retry-count) topic-entity) :retry-count 4)
          consumed-message (rmqw/consume-message nil {:delivery-tag "delivery-tag"} (nippy/freeze message) false)
          converted-message-payload (convert-to-message-payload consumed-message "default")]
      (is (= converted-message-payload expected-message-payload)))))