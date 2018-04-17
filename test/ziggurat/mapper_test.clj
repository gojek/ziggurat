(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.mapper :refer [mapper-func]]
            [lambda-common.metrics :as metrics]
            [sentry.core :refer [sentry-report]]
            [ziggurat.config :refer [config]]
            [langohr.basic :as lb]
            [ziggurat.messsaging.producer :as producer]
            [ziggurat.messsaging.name :as name]
            [ziggurat.fixtures :as fix]
            [taoensso.nippy :as nippy])
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
      (let [unsuccessfully-processed? (atom false)
            retry-fn-called? (atom false)]
        (with-redefs [metrics/message-unsuccessfully-processed! (fn []
                                                                  (reset! unsuccessfully-processed? true))
                      producer/retry (fn [_] (reset! retry-fn-called? true))]
          ((mapper-func (constantly :retry)) message)
          (is (= true @unsuccessfully-processed?))
          (is (= true @retry-fn-called?)))))

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
          expected-message {:foo "bar" :retry-count 4}
          expected-exchange (name/get-with-prepended-app-name (:exchange-name (:delay (:rabbit-mq config))))
          publish-to-delay-queue-called? (atom false)]
      (with-redefs [lb/publish (fn [_ exchange _ actual-message _]
                                 (reset! publish-to-delay-queue-called? true)
                                 (is (= exchange expected-exchange))
                                 (is (Arrays/equals (nippy/freeze expected-message) actual-message)))]
        (producer/retry message)
        (is (= true @publish-to-delay-queue-called?)))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (let [message {:foo "bar" :retry-count 0}
          expected-message (dissoc message :retry-count)
          expected-exchange (name/get-with-prepended-app-name (:exchange-name (:dead-letter (:rabbit-mq config))))
          publish-to-dead-queue-called? (atom false)]
      (with-redefs [lb/publish (fn [_ exchange _ actual-message _]
                                 (reset! publish-to-dead-queue-called? true)
                                 (is (= exchange expected-exchange))
                                 (is (Arrays/equals (nippy/freeze expected-message) actual-message)))]
        (producer/retry message)
        (is (= true @publish-to-dead-queue-called?)))))

  (testing "message with no retry count will publish to delay queue"
    (let [message {:foo "bar"}
          expected-message {:foo "bar" :retry-count 5}
          expected-exchange (name/get-with-prepended-app-name (:exchange-name (:delay (:rabbit-mq config))))
          publish-to-delay-queue-called? (atom false)]
      (with-redefs [lb/publish (fn [_ exchange _ actual-message _]
                                 (reset! publish-to-delay-queue-called? true)
                                 (is (= exchange expected-exchange))
                                 (is (Arrays/equals (nippy/freeze expected-message) actual-message)))]
        (producer/retry message)
        (is (= true @publish-to-delay-queue-called?))))))
