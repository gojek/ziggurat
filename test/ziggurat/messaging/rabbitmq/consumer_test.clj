(ns ziggurat.messaging.rabbitmq.consumer-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.consumer :as rmq-cons]
            [ziggurat.messaging.rabbitmq-wrapper :refer [connection]]
            [ziggurat.messaging.rabbitmq.producer :as rmq-producer]
            [ziggurat.config :refer [rabbitmq-config]]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [langohr.channel :as lch]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as util]
            [ziggurat.messaging.consumer :as consumer]
            [ziggurat.mapper :as mpr])
  (:import (com.rabbitmq.client Channel Connection)))

(use-fixtures :once (join-fixtures [fix/init-messaging
                                    fix/silence-logging]))

(def topic-entity :default)

(defn- gen-message-payload [topic-entity]
  {:message      {:gen-key (apply str (take 10 (repeatedly #(char (+ (rand 26) 65)))))}
   :topic-entity topic-entity})

(defn- create-mock-channel [] (reify Channel
                                (close [_] nil)))

(def message-payload {:foo "bar"})

(deftest consume-message-test
  (testing "It should not call the lb/ack function if ack? is false"
    (let [is-ack-called? (atom false)]
      (with-redefs [rmq-cons/ack-message (fn [_ _] (reset! is-ack-called? true))
                    nippy/thaw           (constantly 1)]
        (rmq-cons/consume-message nil {} (byte-array 12345) false))
      (is (false? @is-ack-called?))))

  (testing "It should call the lb/ack function if ack? is true and return the deserialized message"
    (let [is-ack-called?   (atom false)
          is-nippy-called? (atom false)]
      (with-redefs [rmq-cons/ack-message (fn ([^Channel _ ^long _]
                                              (reset! is-ack-called? true)))
                    nippy/thaw           (fn [payload]
                                           (when (= message-payload payload)
                                             (reset! is-nippy-called? true))
                                           1)]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} message-payload true)]
          (is (= deserialized-message 1))))
      (is (true? @is-ack-called?))
      (is (true? @is-nippy-called?))))

  (testing "It should call the lb/reject function if ack? is false and nippy throws an error"
    (let [is-reject-called? (atom false)]
      (with-redefs [lb/reject  (fn [^Channel _ ^long _ ^Boolean _] (reset! is-reject-called? true))
                    nippy/thaw (fn [_] (throw (Exception. "Deserializaion error")))]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} (byte-array 12345) false)]
          (is (= deserialized-message nil))))
      (is (true? @is-reject-called?))))

  (testing "It should call the lb/reject function if ack? is true and lb/ack function function fails to ack the message"
    (let [is-reject-called? (atom false)]
      (with-redefs [lb/reject            (fn [^Channel _ ^long _ ^Boolean _] (reset! is-reject-called? true))
                    rmq-cons/ack-message (fn [^Channel _ ^long _]
                                           (throw (Exception. "ack error")))
                    nippy/thaw           (constantly 1)]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} (byte-array 12345) true)]
          (is (= deserialized-message nil))))
      (is (true? @is-reject-called?)))))

(deftest get-messages-from-queue-test
  (testing "It should return `count` number of messages from the specified queue"
    (let [count    5
          messages (repeat count message-payload)]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 message-payload])
                    lch/open                 (fn [^Connection _] (create-mock-channel))
                    rmq-cons/consume-message (fn [_ _ ^bytes _ _] message-payload)]
        (let [consumed-messages (rmq-cons/get-messages-from-queue nil "test-queue" true count)]
          (is (= consumed-messages messages))))))

  (testing "It should return `count` number of nils from the specified queue if the payload is empty"
    (let [count    5
          messages (repeat count nil)]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 nil])
                    lch/open                 (fn [^Connection _] (create-mock-channel))
                    rmq-cons/consume-message (fn [_ _ ^bytes _ _] message-payload)]
        (let [consumed-messages (rmq-cons/get-messages-from-queue nil "test-queue" true count)]
          (is (= consumed-messages messages)))))))

(deftest process-messages-from-queue-test
  (testing "The processing function should be called with the correct message and the message is acked"
    (let [count                      5
          times-processing-fn-called (atom 0)
          times-ack-called           (atom 0)
          processing-fn              (fn [message] (when (= message message-payload)
                                                     (swap! times-processing-fn-called inc)))]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 message-payload])
                    lch/open                 (fn [^Connection _] (create-mock-channel))
                    rmq-cons/consume-message (fn [_ _ ^bytes payload _] payload)
                    rmq-cons/ack-message     (fn [_ _] (swap! times-ack-called inc))]
        (rmq-cons/process-messages-from-queue nil "test-queue" count processing-fn))
      (is (= @times-processing-fn-called count))
      (is (= @times-ack-called count))))

  (testing "It should call the lb/reject function when the processing function throws an exception"
    (let [count                5
          reject-fn-call-count (atom 0)
          processing-fn        (fn [_] (throw (Exception. "message processing error")))]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 message-payload])
                    lch/open                 (fn [^Connection _] (create-mock-channel))
                    rmq-cons/reject-message  (fn [_ _ _] (swap! reject-fn-call-count inc))
                    rmq-cons/consume-message (fn [_ _ ^bytes payload _] payload)
                    rmq-cons/ack-message     (fn [_ _] nil)]
        (rmq-cons/process-messages-from-queue nil "test-queue" count processing-fn))
      (is (= @reject-fn-call-count count)))))

(deftest ^:integration start-subscriber-test
  (testing "It should start a RabbitMQ subscriber and consume a message from the instant queue"
    (let [queue-name               "instant-queue-test"
          exchange-name            "instant-queue-exchange"
          is-mocked-mpr-fn-called? (atom false)
          mock-mapper-fn           (fn [message]
                                     (when (= message-payload message)
                                       (reset! is-mocked-mpr-fn-called? true)))]
      (rmq-producer/create-and-bind-queue (rmqw/get-connection) queue-name exchange-name false)
      (rmq-producer/publish (rmqw/get-connection) exchange-name message-payload nil)
      (rmqw/start-subscriber 1 mock-mapper-fn queue-name)
      (Thread/sleep 5000)
      (is (true? @is-mocked-mpr-fn-called?)))))

(deftest ^:integration process-message-test
  (testing "process-message function should ack message after once processing finishes"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message           (gen-message-payload topic-entity)
            processing-fn     (fn [message-arg]
                                (is (= message-arg message)))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open (rmqw/get-connection))]
          (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                prefixed-queue-name (str topic-entity-name "_" queue-name)
                [meta payload] (lb/get ch prefixed-queue-name false)
                _                   (rmq-cons/process-message-from-queue ch meta payload processing-fn)
                consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
            (is (= consumed-message nil)))))))

  (testing "process-message function not process a message if convert-message returns nil"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message              (gen-message-payload topic-entity)
            processing-fn-called (atom false)
            processing-fn        (fn [message-arg]
                                   (if (nil? message-arg)
                                     (reset! processing-fn-called true)))
            topic-entity-name    (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-redefs [consumer/read-messages-from-queue (fn [_ _ _ _] nil)]
          (with-open [ch (lch/open (rmqw/get-connection))]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload] (lb/get ch prefixed-queue-name false)
                  _                   (rmq-cons/process-message-from-queue ch meta payload processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= false @processing-fn-called))
              (is (= consumed-message nil))))))))

  (testing "process-message function should reject and re-queue a message if processing fails"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message           (gen-message-payload topic-entity)
            processing-fn     (fn [message-arg]
                                (is (= message-arg message))
                                (throw (Exception. "exception message")))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open (rmqw/get-connection))]
          (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                prefixed-queue-name (str topic-entity-name "_" queue-name)
                [meta payload] (lb/get ch prefixed-queue-name false)
                _                   (rmq-cons/process-message-from-queue ch meta payload processing-fn)
                consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
            (is (= consumed-message message)))))))

  (testing "process-message function should reject and discard a message if message conversion fails"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message           (gen-message-payload topic-entity)
            processing-fn     (fn [_] ())
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open (rmqw/get-connection))]
          (with-redefs [ziggurat.messaging.consumer/convert-to-message-payload (fn [] (throw (Exception. "exception message")))]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload] (lb/get ch prefixed-queue-name false)
                  _                   (rmq-cons/process-message-from-queue ch meta payload processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= consumed-message nil)))))))))

(deftest ^:integration ack-and-convert-message
  (testing "While constructing a MessagePayload, adds topic-entity as a keyword and retry-count as 0 if message does not already has :retry-count"
    (let [message                   {:foo "bar"}
          expected-message-payload  (assoc (mpr/->MessagePayload (dissoc message :retry-count) topic-entity) :retry-count 0)
          consumed-message          (rmq-cons/consume-message nil {:delivery-tag "delivery-tag"} (nippy/freeze message) false)
          converted-message-payload (consumer/convert-to-message-payload consumed-message "default")]
      (is (= converted-message-payload expected-message-payload))))
  (testing "While constructing a MessagePayload, adds topic-entity as a keyword and retry-count as it exists in the message"
    (let [message                   {:foo "bar" :retry-count 4}
          expected-message-payload  (assoc (mpr/->MessagePayload (dissoc message :retry-count) topic-entity) :retry-count 4)
          consumed-message          (rmq-cons/consume-message nil {:delivery-tag "delivery-tag"} (nippy/freeze message) false)
          converted-message-payload (consumer/convert-to-message-payload consumed-message "default")]
      (is (= converted-message-payload expected-message-payload)))))