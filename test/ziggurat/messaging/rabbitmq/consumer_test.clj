(ns ziggurat.messaging.rabbitmq.consumer-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.consumer :as rmq-cons]
            [ziggurat.messaging.rabbitmq-wrapper :refer [connection]]
            [ziggurat.messaging.rabbitmq.producer :as rmq-producer]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [langohr.channel :as lch])
  (:import (com.rabbitmq.client Channel Connection)
           (java.io Closeable)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq]))

(defn- gen-message-payload [topic-entity]
  {:message      {:gen-key (apply str (take 10 (repeatedly #(char (+ (rand 26) 65)))))}
   :topic-entity topic-entity})

;(deftest start-subsciber-test
;  (testing "It should start a RabbitMQ subscriber and consume a message from the instant queue"
;    (let [queue-name "instant-queue-test"
;          message-atom (atom nil)
;          exchange-name "instant-queue-exchange"
;          message-payload (gen-message-payload "test-topic")
;          mock-mapper-fn (fn [message] (prn "############: arg1" message) (reset! message-atom message))]
;      (rmq-producer/create-and-bind-queue connection queue-name, exchange-name, false)
;      (rmq-producer/publish connection exchange-name message-payload nil)
;      (Thread/sleep 5000)
;      (rmqw/start-subscriber 1  mock-mapper-fn queue-name)
;      (rmqw/stop-connection rmqw/stop-connection connection {:test-topic {:handler-fn #(%)}}))))

(deftest consume-message-test
  (testing "It should not call the lb/ack function if ack? is false"
    (let [is-ack-called? (atom false)]
      (with-redefs [rmq-cons/ack-message (fn [_ _] (reset! is-ack-called? true))
                    nippy/thaw           (constantly 1)]
        (rmq-cons/consume-message nil {} (byte-array 12345) false))
      (is (= @is-ack-called? false))))

  (testing "It should call the lb/ack function if ack? is true and return the deserialized message"
    (let [message-payload  {:message "message"}
          is-ack-called?   (atom false)
          is-nippy-called? (atom false)]
      (with-redefs [rmq-cons/ack-message (fn ([^Channel _ ^long _]
                                              (reset! is-ack-called? true)))
                    nippy/thaw           (fn [payload]
                                           (when (= message-payload payload)
                                             (reset! is-nippy-called? true))
                                           1)]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} message-payload true)]
          (is (= deserialized-message 1))))
      (is (= @is-ack-called? true))
      (is (= @is-nippy-called? true))))

  (testing "It should call the lb/reject function if ack? is false and nippy throws an error"
    (let [is-reject-called? (atom false)]
      (with-redefs [lb/reject  (fn [^Channel _ ^long _ ^Boolean _] (reset! is-reject-called? true))
                    nippy/thaw (fn [_] (throw (Exception. "Deserializaion error")))]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} (byte-array 12345) false)]
          (is (= deserialized-message nil))))
      (is (= @is-reject-called? true))))

  (testing "It should call the lb/reject function if ack? is true and lb/ack function function fails to ack the message"
    (let [is-reject-called? (atom false)]
      (with-redefs [lb/reject            (fn [^Channel _ ^long _ ^Boolean _] (reset! is-reject-called? true))
                    rmq-cons/ack-message (fn [^Channel _ ^long _]
                                           (throw (Exception. "ack error")))
                    nippy/thaw           (constantly 1)]
        (let [deserialized-message (rmq-cons/consume-message nil {:delivery-tag 12345} (byte-array 12345) true)]
          (is (= deserialized-message nil))))
      (is (= @is-reject-called? true)))))

(deftest get-messages-from-queue-test
  (testing "It should return `count` number of messages from the specified queue"
    (let [count    5
          messages (repeat count {:message "message"})]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 {:message "message"}])
                    lch/open                 (fn [^Connection _] (reify Channel
                                                                   (close [this] nil)))
                    rmq-cons/consume-message (fn [_ _ ^bytes _ _] {:message "message"})]
        (let [consumed-messages (rmq-cons/get-messages-from-queue nil "test-queue" true count)]
          (is (= consumed-messages messages))))))

  (testing "It should return `count` number of nils from the specified queue if the payload is empty"
    (let [count    5
          messages (repeat count nil)]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 nil])
                    lch/open                 (fn [^Connection _] (reify Channel
                                                                   (close [this] nil)))
                    rmq-cons/consume-message (fn [_ _ ^bytes _ _] {:message "message"})]
        (let [consumed-messages (rmq-cons/get-messages-from-queue nil "test-queue" true count)]
          (is (= consumed-messages messages)))))))

(deftest process-messages-from-queue-test
  (testing "The processing function should be called with the correct message"
    (let [count           5
          message-payload {:message "message"}
          has-processed?  (atom true)
          processing-fn   (fn [message] (when-not (= message message-payload)
                                          (reset! has-processed? false)))]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 {:message "message"}])
                    lch/open                 (fn [^Connection _] (reify Channel
                                                                   (close [this] nil)))
                    rmq-cons/consume-message (fn [_ _ ^bytes payload _] payload)
                    rmq-cons/ack-message     (fn [_ _] nil)]
        (rmq-cons/process-messages-from-queue nil "test-queue" count processing-fn))
      (is (= @has-processed? true))))

  (testing "It should call the lb/reject function when the processing function throws an exception"
    (let [count                5
          has-processed?       (atom true)
          reject-fn-call-count (atom 0)
          processing-fn        (fn [_] (throw (Exception. "message processing error")))]
      (with-redefs [lb/get                   (fn [^Channel _ ^String _ ^Boolean _]
                                               [1 {:message "message"}])
                    lch/open                 (fn [^Connection _] (reify Channel
                                                                   (close [this] nil)))
                    rmq-cons/reject-message  (fn [_ _ _] (swap! reject-fn-call-count inc))
                    rmq-cons/consume-message (fn [_ _ ^bytes payload _] payload)
                    rmq-cons/ack-message     (fn [_ _] nil)]
        (rmq-cons/process-messages-from-queue nil "test-queue" count processing-fn))
      (is (= @reject-fn-call-count count)))))

