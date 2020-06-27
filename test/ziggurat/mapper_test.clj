(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [sentry-clj.async :refer [sentry-report]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.mapper :refer :all]
            [ziggurat.messaging.rabbitmq-wrapper :refer [connection]]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.rabbitmq :as rmq]
            [langohr.basic :as lb]))

(use-fixtures :once (join-fixtures [fix/init-messaging
                                    fix/silence-logging]))

(deftest ^:integration mapper-func-test
  (let [service-name                    (:app-name (ziggurat-config))
        stream-routes                   {:default {:handler-fn #(constantly nil)}}
        topic-entity                    (name (first (keys stream-routes)))
        message-payload                 {:message {:foo "bar"} :topic-entity (keyword topic-entity)}
        expected-additional-tags        {:topic_name topic-entity}
        expected-metric-namespace       "message-processing"
        report-time-namespace           "handler-fn-execution-time"
        expected-metric-namespaces      [topic-entity expected-metric-namespace]
        expected-report-time-namespaces [topic-entity report-time-namespace]]
    (testing "message process should be successful"
      (let [successfully-processed?     (atom false)
            successfully-reported-time? (atom false)
            expected-metric             "success"]
        (with-redefs [metrics/increment-count  (fn [metric-namespaces metric additional-tags]
                                                 (when (and (or (= metric-namespaces expected-metric-namespaces)
                                                                (= metric-namespaces [expected-metric-namespace]))
                                                            (= metric expected-metric)
                                                            (= additional-tags expected-additional-tags))
                                                   (reset! successfully-processed? true)))
                      metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-report-time-namespaces)
                                                           (= metric-namespaces [report-time-namespace]))
                                                   (reset! successfully-reported-time? true)))]
          ((mapper-func (constantly :success) []) message-payload)
          (is @successfully-processed?)
          (is @successfully-reported-time?))))

    (testing "message process should successfully push to channel queue"
      (fix/with-queues (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (let [successfully-processed? (atom false)
              expected-metric         "success"]
          (with-redefs [metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace [service-name topic-entity expected-metric-namespace])
                                                                 (= metric-namespace [expected-metric-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! successfully-processed? true)))]
            ((mapper-func (constantly :channel-1) [:channel-1]) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-instant-queue topic-entity :channel-1)]
              (is (= message-payload message-from-mq))
              (is @successfully-processed?))))))

    (testing "message process should raise exception if channel not in list"
      (fix/with-queues
        (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (with-redefs [sentry-report (fn [_ _ e & _]
                                      (let [err (Throwable->map e)]
                                        (is (= (:cause err) "Invalid mapper return code"))
                                        (is (= (-> err :data :code) :channel-1))))]
          ((mapper-func (constantly :channel-1) [:some-other-channel]) message-payload)
          (let [message-from-mq (rmq/get-message-from-channel-instant-queue topic-entity :channel-1)]
            (is (nil? message-from-mq))))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              unsuccessfully-processed? (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace [service-name topic-entity expected-metric-namespace])
                                                                 (= metric-namespace [expected-metric-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (constantly :retry) []) message-payload)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic-entity)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "reports error to sentry, publishes message to retry queue if mapper-fn raises exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace [service-name topic-entity expected-metric-namespace])
                                                                 (= metric-namespace [expected-metric-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (fn [_] (throw (Exception. "test exception"))) []) message-payload)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic-entity)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "raises exception when rabbitMQ publishing fails"
      (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
            sentry-report-fn-called?  (atom false)
            unsuccessfully-processed? (atom false)
            expected-metric           "failure"]
        (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                      metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                (when (and (or (= metric-namespace [service-name topic-entity expected-metric-namespace])
                                                               (= metric-namespace [expected-metric-namespace]))
                                                           (= metric expected-metric)
                                                           (= additional-tags expected-additional-tags))
                                                  (reset! unsuccessfully-processed? true)))
                      lb/publish              (fn [_ _ _ _ _]
                                                (throw (Exception. "publishing failure test")))]
          (is (thrown? clojure.lang.ExceptionInfo ((mapper-func (constantly nil) []) message-payload)))
          (is @unsuccessfully-processed?)
          (is @sentry-report-fn-called?))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?   (atom false)
            expected-metric-namespace  "handler-fn-execution-time"
            expected-metric-namespaces [service-name "default" expected-metric-namespace]]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-metric-namespaces)
                                                           (= metric-namespaces [expected-metric-namespace]))
                                                   (reset! reported-execution-time? true)))]
          ((mapper-func (constantly :success) []) message-payload)
          (is @reported-execution-time?))))))

(deftest ^:integration channel-mapper-func-test
  (let [channel                             :channel-1
        channel-name                        (name channel)
        service-name                        (:app-name (ziggurat-config))
        stream-routes                       {:default {:handler-fn #(constantly nil)
                                                       channel     #(constantly nil)}}
        topic                               (first (keys stream-routes))
        message-payload                     {:message      {:foo "bar"}
                                             :retry-count  (:count (:retry (ziggurat-config)))
                                             :topic-entity topic}
        expected-topic-entity-name          (name topic)
        expected-additional-tags            {:topic_name expected-topic-entity-name :channel_name channel-name}
        increment-count-namespace           "message-processing"
        expected-increment-count-namespaces [service-name topic channel-name increment-count-namespace]]
    (testing "message process should be successful"
      (let [successfully-processed? (atom false)
            expected-metric         "success"]
        (with-redefs [metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                (when (and (or (= metric-namespace expected-increment-count-namespaces)
                                                               (= metric-namespace [increment-count-namespace]))
                                                           (= metric expected-metric)
                                                           (= additional-tags expected-additional-tags))
                                                  (reset! successfully-processed? true)))]
          ((channel-mapper-func (constantly :success) channel) message-payload)
          (is @successfully-processed?))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              unsuccessfully-processed? (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace expected-increment-count-namespaces)
                                                                 (= metric-namespace [increment-count-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (constantly :retry) channel) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace expected-increment-count-namespaces)
                                                                 (= metric-namespace [increment-count-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (fn [_] (throw (Exception. "test exception"))) channel) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "raises exception when rabbitMQ publishing fails"
      (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
            sentry-report-fn-called?  (atom false)
            unsuccessfully-processed? (atom false)
            expected-metric           "failure"]
        (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                      metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                (when (and (or (= metric-namespace expected-increment-count-namespaces)
                                                               (= metric-namespace [increment-count-namespace]))
                                                           (= metric expected-metric)
                                                           (= additional-tags expected-additional-tags))
                                                  (reset! unsuccessfully-processed? true)))
                      lb/publish              (fn [_ _ _ _ _]
                                                (throw (Exception. "publishing failure test")))]
          (is (thrown? clojure.lang.ExceptionInfo ((channel-mapper-func (fn [_] (throw (Exception. "test exception"))) channel) message-payload)))
          (is @unsuccessfully-processed?)
          (is @sentry-report-fn-called?))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?           (atom false)
            execution-time-namespace           "execution-time"
            expected-execution-time-namespaces [service-name expected-topic-entity-name channel-name execution-time-namespace]]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-execution-time-namespaces)
                                                           (= metric-namespaces [execution-time-namespace]))
                                                   (reset! reported-execution-time? true)))]
          ((channel-mapper-func (constantly :success) channel) message-payload)
          (is @reported-execution-time?))))))

(deftest message-payload-schema-test
  (testing "it validates the schema for a message containing retry-count"
    (let [message {:message      {:foo "bar"}
                   :topic-entity :topic
                   :retry-count  2}]
      (is (s/validate message-payload-schema message))))
  (testing "It validates the schema for a message that does not contain retry-count"
    (let [message {:message      {:foo "bar"}
                   :topic-entity :topic}]
      (is (s/validate message-payload-schema message))))
  (testing "It raises exception if schema is not correct"
    (let [message {:foo "bar"}]
      (is (thrown? Exception (s/validate message-payload-schema message))))))
