(ns ziggurat.mapper-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]]
            [schema.core :as s]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.mapper :as mapper]
            [ziggurat.message-payload :as mp]
            [ziggurat.metrics :as metrics]
            [ziggurat.util.error :refer [report-error]]
            [ziggurat.util.rabbitmq :refer [bytes-to-str]]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging
                                    fix/mount-metrics]))

(deftest ^:integration mapper-func-test
  (let [service-name                    (:app-name (ziggurat-config))
        stream-routes                   {:default {:handler-fn #(constantly nil)}}
        topic-entity                    (name (first (keys stream-routes)))
        message-payload                 {:message (.getBytes "foo-bar") :topic-entity (keyword topic-entity) :retry-count 5}
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
          ((mapper/mapper-func (constantly :success) []) message-payload)
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
            ((mapper/mapper-func (constantly :channel-1) [:channel-1]) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-instant-queue topic-entity :channel-1)]
              (is (= (bytes-to-str message-payload) (bytes-to-str  message-from-mq)))
              (is @successfully-processed?))))))

    (testing "message process should raise exception if channel not in list"
      (fix/with-queues
        (assoc-in stream-routes [:default :channel-1] (constantly :success))
        (with-redefs [report-error (fn [e _]
                                     (let [err (Throwable->map e)]
                                       (is (= (:cause err) "Invalid mapper return code"))
                                       (is (= (-> err :data :code) :channel-1))))]
          ((mapper/mapper-func (constantly :channel-1) [:some-other-channel]) message-payload)
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
            ((mapper/mapper-func (constantly :retry) []) message-payload)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic-entity)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message))))
            (is @unsuccessfully-processed?)))))

    (testing "reports error, publishes message to retry queue if mapper-fn raises exception"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              report-fn-called?         (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [report-error            (fn [_ _] (reset! report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace [service-name topic-entity expected-metric-namespace])
                                                                 (= metric-namespace [expected-metric-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper/mapper-func (fn [_] (throw (Exception. "test exception"))) []) message-payload)
            (let [message-from-mq (rmq/get-msg-from-delay-queue topic-entity)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message))))
            (is @unsuccessfully-processed?)
            (is @report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?   (atom false)
            expected-metric-namespace  "handler-fn-execution-time"
            expected-metric-namespaces [service-name "default" expected-metric-namespace]]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-metric-namespaces)
                                                           (= metric-namespaces [expected-metric-namespace]))
                                                   (reset! reported-execution-time? true)))]
          ((mapper/mapper-func (constantly :success) []) message-payload)
          (is @reported-execution-time?))))))

(deftest ^:integration channel-mapper-func-test
  (let [channel                             :channel-1
        channel-name                        (name channel)
        service-name                        (:app-name (ziggurat-config))
        stream-routes                       {:default {:handler-fn #(constantly nil)
                                                       channel     #(constantly nil)}}
        topic                               (first (keys stream-routes))
        message-payload                     {:message      (.getBytes "foo-bar")
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
          ((mapper/channel-mapper-func (constantly :success) channel) message-payload)
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
            ((mapper/channel-mapper-func (constantly :retry) channel) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message))))
            (is @unsuccessfully-processed?)))))

    (testing "message should raise exception and report the error"
      (fix/with-queues stream-routes
        (let [expected-message          (assoc message-payload :retry-count (dec (:count (:retry (ziggurat-config)))))
              report-fn-called?         (atom false)
              unsuccessfully-processed? (atom false)
              expected-metric           "failure"]
          (with-redefs [report-error            (fn [_ _] (reset! report-fn-called? true))
                        metrics/increment-count (fn [metric-namespace metric additional-tags]
                                                  (when (and (or (= metric-namespace expected-increment-count-namespaces)
                                                                 (= metric-namespace [increment-count-namespace]))
                                                             (= metric expected-metric)
                                                             (= additional-tags expected-additional-tags))
                                                    (reset! unsuccessfully-processed? true)))]
            ((mapper/channel-mapper-func (fn [_] (throw (Exception. "test exception"))) channel) message-payload)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic channel)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message))))
            (is @unsuccessfully-processed?)
            (is @report-fn-called?)))))

    (testing "reports execution time with topic prefix"
      (let [reported-execution-time?           (atom false)
            execution-time-namespace           "execution-time"
            expected-execution-time-namespaces [service-name expected-topic-entity-name channel-name execution-time-namespace]]
        (with-redefs [metrics/report-histogram (fn [metric-namespaces _ _]
                                                 (when (or (= metric-namespaces expected-execution-time-namespaces)
                                                           (= metric-namespaces [execution-time-namespace]))
                                                   (reset! reported-execution-time? true)))]
          ((mapper/channel-mapper-func (constantly :success) channel) message-payload)
          (is @reported-execution-time?))))))
