(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all])
  (:require [lambda-common.metrics :as metrics]
            [sentry.core :refer [sentry-report]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.mapper :refer [mapper-func]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq])
  (:import (java.util Arrays)))

(use-fixtures :once fix/init-rabbit-mq)

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
          (let [message-from-mq (rmq/get-msg-from-delay-queue)]
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