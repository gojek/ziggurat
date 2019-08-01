(ns ziggurat.mapper-test
  (:require [clojure.test :refer :all])
  (:require [langohr.channel :as lch]
            [sentry-clj.async :refer [sentry-report]]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.mapper :refer [mapper-func channel-mapper-func]]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest mapper-func-test
  (let [service-name                    (:app-name (ziggurat-config))
        message                         {:foo "bar"}
        stream-routes                   {:default {:handler-fn #(constantly nil)}}
        expected-topic-entity-name      (name (first (keys stream-routes)))
        expected-additional-tags        {:topic_name expected-topic-entity-name}
        default-namespace               "message-processing"
        report-time-namespace           "handler-fn-execution-time"
        expected-metric-namespaces      [expected-topic-entity-name default-namespace]
        expected-report-time-namespaces [expected-topic-entity-name report-time-namespace]]
    (testing "message process should be successful"
      (let [successfully-processed?     (atom false)
            successfully-reported-time? (atom false)
            expected-metric             "success"]
        (with-redefs [metrics/increment-count  (fn [metric-namespaces metric additional-tags]
                                                 (when (and (or (= metric-namespaces expected-metric-namespaces)
                                                                (= metric-namespaces [default-namespace]))
                                                            (= metric expected-metric)
                                                            (= additional-tags expected-additional-tags))
                                                   (reset! successfully-processed? true)))
                      metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-report-time-namespaces)
                                                           (= metric-namespaces [report-time-namespace]))
                                                   (reset! successfully-reported-time? true)))]
          ((mapper-func (constantly :success) expected-topic-entity-name []) message)
          (is @successfully-processed?)
          (is @successfully-reported-time?))))

    (testing "message process should successfully push to channel queue"
      (fix/with-queues (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (let [successfully-processed? (atom false)
              expected-metric         "success"]
          (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces [service-name expected-topic-entity-name default-namespace])
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! successfully-processed? true)))]
            ((mapper-func (constantly :channel-1) expected-topic-entity-name [:channel-1]) message)
            (let [message-from-mq (rmq/get-message-from-channel-instant-queue expected-topic-entity-name :channel-1)]
              (is (= message message-from-mq))
              (is @successfully-processed?))))))

    (testing "message process should successfully push to delay channel queue"
      (fix/with-queues
        (assoc-in stream-routes [:default :channels :channel-1 :queue-timeout-ms] 1000)
        (let [successfully-processed? (atom false)
              expected-metric         "success"]
          (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces [service-name expected-topic-entity-name default-namespace])
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! successfully-processed? true)))]
            ((mapper-func (constantly :channel-1) expected-topic-entity-name [:channel-1]) message)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue expected-topic-entity-name :channel-1)]
              (is (= message message-from-mq))
              (is @successfully-processed?))))))

    (testing "message process should raise exception if channel not in list"
      (fix/with-queues
        (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (with-redefs [sentry-report (fn [_ _ e & _]
                                      (let [err (Throwable->map e)]
                                        (is (= (:cause err) "Invalid mapper return code"))
                                        (is (= (-> err :data :code) :channel-1))))]
          ((mapper-func (constantly :channel-1) expected-topic-entity-name [:some-other-channel]) message)
          (let [message-from-mq (rmq/get-message-from-channel-instant-queue expected-topic-entity-name :channel-1)]
            (is (nil? message-from-mq))))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (dec (:count (:retry (ziggurat-config)))))
              unsuccessfully-processed? (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces [service-name expected-topic-entity-name default-namespace])
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (constantly :retry) expected-topic-entity-name []) message)
            (let [message-from-mq (rmq/get-msg-from-delay-queue expected-topic-entity-name)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (dec (:count (:retry (ziggurat-config)))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces [service-name expected-topic-entity-name default-namespace])
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper-func (fn [_] (throw (Exception. "test exception"))) expected-topic-entity-name []) message)
            (let [message-from-mq (rmq/get-msg-from-delay-queue expected-topic-entity-name)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?   (atom false)
            execution-time-namesapce   "handler-fn-execution-time"
            expected-metric-namespaces [service-name "default" execution-time-namesapce]]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (is (or (= metric-namespaces expected-metric-namespaces)
                                                         (= metric-namespaces [execution-time-namesapce])))
                                                 (when (or (= metric-namespaces expected-metric-namespaces)
                                                           (= metric-namespaces [execution-time-namesapce]))
                                                   (reset! reported-execution-time? true)))]

          ((mapper-func (constantly :success) expected-topic-entity-name []) message)
          (is @reported-execution-time?))))))

(deftest channel-mapper-func-test
  (let [service-name               (:app-name (ziggurat-config))
        message                    {:foo "bar"}
        stream-routes              {:default {:handler-fn #(constantly nil)
                                              :channel-1  #(constantly nil)}}
        topic                      (first (keys stream-routes))
        expected-topic-entity-name (name topic)
        expected-additional-tags   {:topic_name expected-topic-entity-name}
        channel                    :channel-1
        channel-name               (name channel)
        default-namespace          "message-processing"
        expected-metric-namespaces [expected-topic-entity-name channel default-namespace]]
    (testing "message process should be successful"
      (let [successfully-processed? (atom false)
            expected-metric         "success"]
        (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                (when (and (or (= metric-namespaces [service-name expected-topic-entity-name channel-name default-namespace])
                                                               (= metric-namespaces [default-namespace]))
                                                           (= metric expected-metric)
                                                           (= additional-tags expected-additional-tags))
                                                  (reset! successfully-processed? true)))]
          ((channel-mapper-func (constantly :success) topic channel) message)
          (is @successfully-processed?))))

    (testing "message process should be unsuccessful and retry"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (dec (:count (:retry (ziggurat-config)))))
              unsuccessfully-processed? (atom false)
              expected-metric           "retry"]

          (with-redefs [metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces [service-name expected-topic-entity-name channel-name default-namespace])
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (constantly :retry) topic channel) message)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message :retry-count (dec (:count (:retry (ziggurat-config)))))
              sentry-report-fn-called?  (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [sentry-report           (fn [_ _ _ & _] (reset! sentry-report-fn-called? true))
                        metrics/increment-count (fn [metric-namespaces metric additional-tags]
                                                  (when (and (or (= metric-namespaces expected-metric-namespaces)
                                                                 (= metric-namespaces [default-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((channel-mapper-func (fn [_] (throw (Exception. "test exception"))) topic channel) message)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= message-from-mq expected-message)))
            (is @unsuccessfully-processed?)
            (is @sentry-report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time? (atom false)
            execution-time-namesapce "execution-time"]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces [service-name expected-topic-entity-name channel-name execution-time-namesapce])
                                                           (= metric-namespaces [execution-time-namesapce]))
                                                   (reset! reported-execution-time? true)))]
          ((channel-mapper-func (constantly :success) topic channel) message)
          (is @reported-execution-time?))))))
