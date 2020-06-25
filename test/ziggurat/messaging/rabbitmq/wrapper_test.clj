(ns ziggurat.messaging.rabbitmq.wrapper-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.connection :as rmq-connection]
            [ziggurat.fixtures :as fix]))

(use-fixtures :once fix/mount-only-config)

(defn create-mock-object [] (reify Object
                              (toString [this] "")))

(defn reset-connection-atom [] (reset! rmqw/connection nil))

(deftest start-connection-test
  (testing "start-connection should call the rmq-connection/start-connection and return the connection atom"
    (let [default-config                config/config
          start-connection-called-count (atom false)
          stream-routes                 {:default {:handler-fn (constantly nil)}}
          config                        (assoc default-config
                                          :ziggurat {:retry {:enabled true}})]
      (with-redefs [rmq-connection/start-connection (fn [_] (reset! start-connection-called-count true) {:foo "bar"})]
        (rmqw/start-connection config stream-routes)
        (is (= true @start-connection-called-count))
        (is (= {:foo "bar"} @rmqw/connection)))
      (reset-connection-atom)))

  (testing "start-connection should not call rmq-connection/start-connection function if retries are disabled and connection atom should be nil"
    (let [default-config           config/config
          start-connection-called? (atom false)
          stream-routes            {:default {:handler-fn (constantly nil)}}
          config                   (assoc default-config
                                     :ziggurat {:retry {:enabled false}})]
      (with-redefs [rmq-connection/start-connection (fn [_] (reset! start-connection-called? true))]
        (rmqw/start-connection config stream-routes)
        (is (= false @start-connection-called?))
        (is (= @rmqw/connection nil))))
    (reset-connection-atom)))

(deftest stop-connection-test
  (testing "stop-connection should stop the connection if retries are enabled and connection is not nil"
    (let [default-config          config/config
          stop-connection-called? (atom false)
          stream-routes           {:default {:handler-fn (constantly nil)}}
          config                  (assoc default-config
                                    :ziggurat {:retry {:enabled true}})]
      (with-redefs [rmq-connection/stop-connection (fn [_ _] (reset! stop-connection-called? true))
                    rmqw/connection                (atom {:foo "bar"})]
        (rmqw/stop-connection config stream-routes)
        (is (= true @stop-connection-called?))))
    (reset-connection-atom))

  (testing "stop-connection should not call the rmq-connection/stop-connection function if connection atom is nil"
    (let [default-config          config/config
          stop-connection-called? (atom false)
          stream-routes           {:default {:handler-fn (constantly nil)}}
          config                  (assoc default-config
                                    :ziggurat {:retry {:enabled true}})]
      (with-redefs [rmq-connection/stop-connection (fn [_ _] (reset! stop-connection-called? true))
                    rmqw/connection                (atom nil)]
        (rmqw/stop-connection config stream-routes)
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
      (with-redefs [rmq-connection/start-connection (fn [_] (reset! start-connection-called-count true) mock-object)]
        (rmqw/start-connection config stream-routes)
        (rmqw/start-connection config stream-routes)
        (rmqw/start-connection config stream-routes)
        (is (= true @start-connection-called-count))
        (is (= mock-object @rmqw/connection)))
      (reset-connection-atom))))

(deftest stop-connection-idempotency-test
  (testing "It should not set the connection atom if it has already been set"
    (let [default-config               config/config
          stop-connection-called-count (atom 0)
          stream-routes                {:default {:handler-fn (constantly nil)}}
          config                       (assoc default-config
                                         :ziggurat {:retry {:enabled true}})]
      (with-redefs [rmqw/connection                (atom {:foo "bar"})
                    rmq-connection/stop-connection (fn [_ _] (swap! stop-connection-called-count inc))]
        (rmqw/stop-connection config stream-routes)
        (rmqw/stop-connection config stream-routes)
        (rmqw/stop-connection config stream-routes)
        (is (= 1 @stop-connection-called-count))
        (is (= nil @rmqw/connection)))
      (reset-connection-atom))))



