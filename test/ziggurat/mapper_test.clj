(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.mapper :refer [mapper-func]]
            [lambda-common.metrics :as metrics]
            [sentry.core :refer [sentry-report]]
            [ziggurat.config :refer [rabbitmq-config ziggurat-config]]
            [ziggurat.messaging.connection :refer [connection]]
            [langohr.basic :as lb]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.fixtures :as fix]
            [taoensso.nippy :as nippy]
            [langohr.channel :as lch]
            [ziggurat.messaging.consumer :as consumer])
  (:import (java.util Arrays)))

(use-fixtures :once fix/init-rabbit-mq)

(defn get-msg-from-rabbitmq [queue-name]
  (with-open [ch (lch/open connection)]
    (let [[meta payload] (lb/get ch queue-name false)]
      (consumer/convert-and-ack-message ch meta payload))))

(defn- get-msg-from-delay-queue []
  (let [{:keys [queue-name queue-timeout-ms]} (:delay (rabbitmq-config))
        queue-name (producer/delay-queue-name queue-name queue-timeout-ms)]
    (get-msg-from-rabbitmq queue-name)))

(defn- get-msg-from-dead-queue []
  (let [{:keys [queue-name]} (:dead-letter (rabbitmq-config))]
    (get-msg-from-rabbitmq queue-name)))

(defn- get-msg-from-instant-queue []
  (let [{:keys [queue-name]} (:instant (rabbitmq-config))]
    (get-msg-from-rabbitmq queue-name)))

(deftest mapper-func-test
  (let [message {:foo "bar"}]
    (testing "message process should be successful"
      (let [successfully-processed? (atom false)]
        (with-redefs [metrics/message-successfully-processed! (fn []
                                                                (reset! successfully-processed? true))]
          ((mapper-func (constantly :success)) message)
          (is (= true @successfully-processed?)))))

    (testing "message process should be unsuccessful and retry"
      (let [expected-message (assoc message :retry-count (:count (:retry (ziggurat-config))))
            unsuccessfully-processed? (atom false)
            retry-fn-called? (atom false)]
        (with-redefs [metrics/message-unsuccessfully-processed! (fn []
                                                                  (reset! unsuccessfully-processed? true))]
          ((mapper-func (constantly :retry)) message)
          (let [message-from-mq (get-msg-from-delay-queue)]
            (is (= message-from-mq expected-message)))
          (is (= true @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (let [sentry-report-fn-called? (atom false)
            message-unsuccessfully-processed-fn-called? (atom false)]
        (with-redefs [sentry-report (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                      metrics/message-unsuccessfully-processed! (fn [] (reset! message-unsuccessfully-processed-fn-called? true))]
          ((mapper-func (fn [_] (throw (Exception. "test exception"))))
            message)
          (is (= true @message-unsuccessfully-processed-fn-called?))
          (is (= true @sentry-report-fn-called?)))))))

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (let [message {:foo "bar" :retry-count 5}
          expected-message {:foo "bar" :retry-count 4}]
      (producer/retry message)
      (let [message-from-mq (get-msg-from-delay-queue)]
        (is (= expected-message message-from-mq)))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (let [message {:foo "bar" :retry-count 0}
          expected-message (dissoc message :retry-count)]
      (producer/retry message)
      (let [message-from-mq (get-msg-from-dead-queue)]
        (is (= expected-message message-from-mq)))))

  (testing "message with no retry count will publish to delay queue"
    (let [message {:foo "bar"}
          expected-message {:foo "bar" :retry-count 5}]
      (producer/retry message)
      (let [message-from-mq (get-msg-from-delay-queue)]
        (is (= message-from-mq expected-message))))))
