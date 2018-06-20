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
  (let [message                   {:foo "bar"}
        topic                     "booking"
        expected-metric-namespace "booking.message-processing"]
    (testing "message process should be successful"
      (let [successfully-processed?  (atom false)
            expected-metric          "success"]
        (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                (do (is (= metric-namespace expected-metric-namespace))
                                                    (is (= metric expected-metric))
                                                    (reset! successfully-processed? true)))]
          ((mapper-func (constantly :success)) message topic)
          (is (= true @successfully-processed?)))))

    (testing "message process should be unsuccessful and retry"
      (let [expected-message (assoc message :retry-count (:count (:retry (ziggurat-config))))
            unsuccessfully-processed? (atom false)
            retry-fn-called? (atom false)
            expected-metric "failure"]

        (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                (do (is (= metric-namespace expected-metric-namespace))
                                                    (is (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
          ((mapper-func (constantly :retry)) message topic)
          (let [message-from-mq (rmq/get-msg-from-delay-queue topic)]
            (is (= message-from-mq expected-message)))
          (is (= true @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (let [sentry-report-fn-called?  (atom false)
            unsuccessfully-processed? (atom false)
            expected-metric           "failure"]
        (with-redefs [sentry-report (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                      metrics/increment-count (fn [metric-namespace metric]
                                                (do (is (= metric-namespace expected-metric-namespace))
                                                    (is (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
          ((mapper-func (fn [_] (throw (Exception. "test exception"))))
            message topic)
          (is (= true @unsuccessfully-processed?))
          (is (= true @sentry-report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time? (atom false)
            expected-metric-namespace "booking.handler-fn-execution-time"]
        (with-redefs [metrics/report-time (fn [metric-namespace _] (do (is (= metric-namespace expected-metric-namespace))
                                                                       (reset! reported-execution-time? true)))]

          ((mapper-func (constantly :success)) message topic)
          (is (= true @reported-execution-time?)))))))
