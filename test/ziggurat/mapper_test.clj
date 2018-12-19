(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all])
  (:require [langohr.channel :as lch]
            [sentry-clj.async :refer [sentry-report]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.mapper :refer [mapper-func channel-mapper-func]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.rabbitmq :as rmq])
  (:import (java.util Arrays)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest mapper-func-test
  (let [message                        {:foo "bar"}
        stream-routes                  {:default {:handler-fn #(constantly nil)}}
        topic                          (name (first (keys stream-routes)))
        expected-metric-namespace      "default.message-processing"
        expected-report-time-namespace "default.handler-fn-execution-time"]
    (testing "message process should be successful"
      (let [successfully-processed?     (atom false)
            successfully-reported-time? (atom false)
            expected-metric             "success"]
        (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                (when (and (= metric-namespace expected-metric-namespace)
                                                           (= metric expected-metric))
                                                  (reset! successfully-processed? true)))
                      metrics/report-time     (fn [metric-namespace _]
                                                (when (= metric-namespace expected-report-time-namespace)
                                                  (reset! successfully-reported-time? true)))]
          ((mapper-func (constantly :success) topic []) message)
          (is @successfully-processed?)
          (is @successfully-reported-time?))))

    (testing "message process should successfully push to channel queue"
      (fix/with-queues (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (let [successfully-processed? (atom false)
              expected-metric         "success"]
          (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                  (when (and (= metric-namespace expected-metric-namespace)
                                                             (= metric expected-metric))
                                                    (reset! successfully-processed? true)))]
            ((mapper-func (constantly :channel-1) topic [:channel-1]) message)
            (let [message-from-mq (rmq/get-message-from-channel-instant-queue topic :channel-1)]
              (is (= message message-from-mq))
              (is @successfully-processed?))))))

    (testing "message process should raise exception if channel not in list"
      (fix/with-queues (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (let [successfully-processed? (atom false)
              expected-metric         "success"]
          (with-redefs [sentry-report (fn [_ _ e & _]
                                        (let [err (Throwable->map e)]
                                          (is (= (:cause err) "Invalid mapper return code"))
                                          (is (= (-> err :data :code) :channel-1))))]
            ((mapper-func (constantly :channel-1) topic [:some-other-channel]) message)
            (let [message-from-mq (rmq/get-message-from-channel-instant-queue topic :channel-1)]
              (is (nil? message-from-mq)))))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (:count (:retry (ziggurat-config))))
              unsuccessfully-processed? (atom false)
              retry-fn-called?          (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                  (when (and (= metric-namespace expected-metric-namespace)
                                                             (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (constantly :retry) topic []) message)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (:count (:retry (ziggurat-config))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric]
                                                  (when (and (= metric-namespace expected-metric-namespace)
                                                             (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (fn [_] (throw (Exception. "test exception"))) topic []) message)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?  (atom false)
            expected-metric-namespace "default.handler-fn-execution-time"]
        (with-redefs [metrics/report-time (fn [metric-namespace _]
                                            (when (= metric-namespace expected-metric-namespace)
                                              (reset! reported-execution-time? true)))]

          ((mapper-func (constantly :success) topic []) message)
          (is @reported-execution-time?))))))

(deftest channel-mapper-func-test
  (let [message                   {:foo "bar"}
        stream-routes             {:default {:handler-fn #(constantly nil)
                                             :channel-1  #(constantly nil)}}
        topic                     (first (keys stream-routes))
        channel                   :channel-1
        expected-metric-namespace "default.channel-1.message-processing"]
    (testing "message process should be successful"
      (let [successfully-processed? (atom false)
            expected-metric         "success"]
        (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                (when (and (= metric-namespace expected-metric-namespace)
                                                           (= metric expected-metric))
                                                  (reset! successfully-processed? true)))]
          ((channel-mapper-func (constantly :success) topic channel) message)
          (is @successfully-processed?))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (:count (:retry (ziggurat-config))))
              unsuccessfully-processed? (atom false)
              retry-fn-called?          (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespace metric]
                                                  (when (and (= metric-namespace expected-metric-namespace)
                                                             (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (constantly :retry) topic channel) message)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (:count (:retry (ziggurat-config))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric]
                                                  (when (and (= metric-namespace expected-metric-namespace)
                                                             (= metric expected-metric))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (fn [_] (throw (Exception. "test exception"))) topic channel) message)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?  (atom false)
            expected-metric-namespace "default.channel-1.execution-time"]
        (with-redefs [metrics/report-time (fn [metric-namespace _]
                                            (when (= metric-namespace expected-metric-namespace)
                                              (reset! reported-execution-time? true)))]

          ((channel-mapper-func (constantly :success) topic channel) message)
          (is @reported-execution-time?))))))
