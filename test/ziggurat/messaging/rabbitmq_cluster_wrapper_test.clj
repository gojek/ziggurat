(ns ziggurat.messaging.rabbitmq-cluster-wrapper-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.rabbitmq-cluster-wrapper :as rmqcw]
            [ziggurat.messaging.rabbitmq.cluster.connection :as rmq-cluster-connection]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :as config]
            [ziggurat.messaging.rabbitmq.cluster.producer :as rmqc-producer]
            [ziggurat.messaging.rabbitmq.producer :as rmq-producer]
            [ziggurat.messaging.rabbitmq.consumer :as rmq-consumer]))

(use-fixtures :once fix/mount-only-config)

(def rmq-cluster-config {:hosts "localhost"
                         :port 5672
                         :username "guest"
                         :password "guest"
                         :channel-timeout 2000
                         :ha-mode "all"
                         :ha-sync-mode "automatic"})

(defn- create-mock-object [] (reify Object
                               (toString [this] "")))

(defn reset-connection-atom [] (reset! rmqcw/connection nil))

(deftest start-connection-test
  (testing "start-connection should call the `rmq-cluster-connection/start-connection` and set the connection and config atoms only if connection atom is nil"
    (let [default-config                config/config
          start-connection-called-count (atom false)
          stream-routes                 {:default {:handler-fn (constantly nil)}}
          config                        (assoc default-config
                                               :ziggurat {:rabbit-mq-connection rmq-cluster-config})]
      (with-redefs [rmqcw/connection (atom nil)
                    rmq-cluster-connection/start-connection (fn [_] (reset! start-connection-called-count true) {:foo "bar"})]
        (rmqcw/start-connection config stream-routes)
        (is (= true @start-connection-called-count))
        (is (= @rmqcw/rabbitmq-cluster-config rmq-cluster-config))
        (is (= {:foo "bar"} (rmqcw/get-connection))))
      (reset-connection-atom))))

(deftest stop-connection-test
  (testing "stop-connection should stop the only connection when `get-connection` returns a non-nil value"
    (let [stop-connection-called? (atom false)
          stream-routes           {:default {:handler-fn (constantly nil)}}]
      (with-redefs [rmq-cluster-connection/stop-connection (fn [_ _] (reset! stop-connection-called? true))
                    rmqcw/get-connection                   (constantly {:foo "bar"})]
        (rmqcw/stop-connection config/config stream-routes)
        (is (= true @stop-connection-called?))))
    (reset-connection-atom))

  (testing "stop-connection should not call the `rmq-connection/stop-connection` function if connection atom is nil"
    (let [default-config          config/config
          stop-connection-called? (atom false)
          stream-routes           {:default {:handler-fn (constantly nil)}}
          config                  (assoc default-config
                                         :ziggurat {:rabbit-mq-connection rmq-cluster-config})]
      (with-redefs [rmq-cluster-connection/stop-connection (fn [_ _] (reset! stop-connection-called? true))
                    rmqcw/get-connection                (constantly nil)]
        (rmqcw/stop-connection config stream-routes)
        (is (= false @stop-connection-called?))))
    (reset-connection-atom)))

(deftest start-connection-idempotency-test
  (testing "It should not set the connection atom if it has already been set"
    (let [default-config                config/config
          start-connection-called-count (atom false)
          mock-object                   (create-mock-object)
          stream-routes                 {:default {:handler-fn (constantly nil)}}
          config                        (assoc default-config
                                               :ziggurat {:retry {:enabled true}})]
      (with-redefs [rmq-cluster-connection/start-connection (fn [_] (reset! start-connection-called-count true) mock-object)]
        (rmqcw/start-connection config stream-routes)
        (rmqcw/start-connection config stream-routes)
        (rmqcw/start-connection config stream-routes)
        (is (= true @start-connection-called-count))
        (is (= mock-object (rmqcw/get-connection))))
      (reset-connection-atom))))

(deftest stop-connection-idempotency-test
  (testing "It should not reset the connection atom if connection has already been stopped"
    (let [stop-connection-called-count (atom 0)
          stream-routes                {:default {:handler-fn (constantly nil)}}]
      (with-redefs [rmqcw/get-connection                (constantly nil)
                    rmq-cluster-connection/stop-connection (fn [_ _] (swap! stop-connection-called-count inc))]
        (rmqcw/stop-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= 0 @stop-connection-called-count)))
      (reset-connection-atom))))

(deftest create-and-bind-queue-test
  (testing "It should call the create `rmq-producer/create-and-bind-queue` function without dead-letter-exchange"
    (let [test-queue-name               "test-queue"
          test-exchange-name            "test-exchange"
          create-and-bind-queue-called? (atom false)]
      (with-redefs [rmqc-producer/create-and-bind-queue (fn [config _ queue-name exchange-name dead-letter-exchange]
                                                          (when (and (= queue-name test-queue-name)
                                                                     (= exchange-name test-exchange-name)
                                                                     (= config rmq-cluster-config config)
                                                                     (= dead-letter-exchange nil))
                                                            (reset! create-and-bind-queue-called? true)))]
        (rmqcw/create-and-bind-queue test-queue-name test-exchange-name)
        (is (= @create-and-bind-queue-called? true)))))

  (testing "It should call the `create rmq-producer/create-and-bind-queue` function with dead-letter-exchange"
    (let [test-queue-name               "test-queue"
          test-exchange-name            "test-exchange"
          dead-letter-exchange-name     "test-dead-letter-exchange"
          create-and-bind-queue-called? (atom false)]
      (with-redefs [rmqc-producer/create-and-bind-queue (fn [config _ queue-name exchange-name dead-letter-exchange]
                                                          (when (and (= queue-name test-queue-name)
                                                                     (= exchange-name test-exchange-name)
                                                                     (= config rmq-cluster-config)
                                                                     (= dead-letter-exchange dead-letter-exchange-name))
                                                            (reset! create-and-bind-queue-called? true)))]
        (rmqcw/create-and-bind-queue test-queue-name test-exchange-name dead-letter-exchange-name)
        (is (= @create-and-bind-queue-called? true))))))

(deftest publish-test
  (testing "it should call `rmq-producer/publish` without expiration"
    (let [test-exchange-name   "test-exchange"
          test-message-payload {:foo "bar"}
          publish-called?      (atom false)]
      (with-redefs [rmq-producer/publish (fn [_ exchange message-payload expiration]
                                           (when (and (= exchange test-exchange-name)
                                                      (= message-payload test-message-payload)
                                                      (= expiration nil))
                                             (reset! publish-called? true)))]
        (rmqcw/publish test-exchange-name test-message-payload)
        (is (= @publish-called? true)))))

  (testing "It should call `rmq-producer/publish` with expiration"
    (let [test-exchange-name   "test-exchange"
          test-message-payload {:foo "bar"}
          test-expiration      "42"
          publish-called?      (atom false)]
      (with-redefs [rmq-producer/publish (fn [_ exchange message-payload expiration]
                                           (when (and (= exchange test-exchange-name)
                                                      (= message-payload test-message-payload)
                                                      (= expiration test-expiration))
                                             (reset! publish-called? true)))]
        (rmqcw/publish test-exchange-name test-message-payload test-expiration)
        (is (= @publish-called? true))))))

(deftest get-messages-from-queue-test
  (testing "it should call `rmq-consumer/get-messages-from-queue` with a default `count` of 1 when count is not specified"
    (let [test-queue-name                 "test-queue"
          default-count                   1
          get-messages-from-queue-called? (atom false)]
      (with-redefs [rmq-consumer/get-messages-from-queue (fn [_ queue-name ack? count]
                                                           (when (and (= test-queue-name queue-name)
                                                                      (= default-count count)
                                                                      (= ack? true)))
                                                           (reset! get-messages-from-queue-called? true))]
        (rmqcw/get-messages-from-queue test-queue-name true))))

  (testing "it should call `rmq-consumer/get-messages-from-queue` when `count` is specified"
    (let [test-queue-name                 "test-queue"
          test-count                      5
          get-messages-from-queue-called? (atom false)]
      (with-redefs [rmq-consumer/get-messages-from-queue (fn [_ queue-name ack? count]
                                                           (when (and (= test-queue-name queue-name)
                                                                      (= test-count count)
                                                                      (= ack? true)))
                                                           (reset! get-messages-from-queue-called? true))]
        (rmqcw/get-messages-from-queue test-queue-name true test-count)))))

(deftest process-messages-from-queue-test
  (testing "It should call `rmq-consumer/process-messages-from-queue` with the correct arguments"
    (let [test-queue                          "test-queue"
          test-count                          5
          test-processing-fn                  (constantly {:foo "bar"})
          process-messages-from-queue-called? (atom false)]
      (with-redefs [rmq-consumer/process-messages-from-queue (fn [_ queue-name count processing-fn]
                                                               (when (and (= queue-name test-queue)
                                                                          (= count test-count)
                                                                          (= {:foo "bar"} (processing-fn)))
                                                                 (reset! process-messages-from-queue-called? true)))]
        (rmqcw/process-messages-from-queue test-queue test-count test-processing-fn)
        (is (= true @process-messages-from-queue-called?))))))

(deftest start-subscriber-test
  (testing "it should call `rmq-consumer/start-subscriber` with the right arguments"
    (let [test-prefetch-count      5
          test-queue-name          "test-queue"
          test-mapper-fn           (constantly {:foo "bar"})
          start-subscriber-called? (atom false)]
      (with-redefs [rmq-consumer/start-subscriber (fn [_ prefetch-count wrapped-mapper-fn queue-name]
                                                    (when (and (= queue-name test-queue-name)
                                                               (= test-prefetch-count prefetch-count)
                                                               (= (wrapped-mapper-fn) {:foo "bar"}))
                                                      (reset! start-subscriber-called? true)))]
        (rmqcw/start-subscriber test-prefetch-count test-mapper-fn test-queue-name)))))

(deftest consume-message-test
  (testing "it should call `rmq-consumer/consume-message` with the correct arguments"
    (let [test-meta    {:meta "bar"}
          test-payload {:foo "bar"}
          test-ack?    false]
      (with-redefs [rmq-consumer/consume-message (fn [_ meta ^bytes payload ack?]
                                                   (when (and (= meta test-meta)
                                                              (= payload test-payload)
                                                              (= ack? test-ack?))))]
        (rmqcw/consume-message nil test-meta test-payload test-ack?)))))

