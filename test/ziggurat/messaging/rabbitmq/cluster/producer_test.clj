(ns ziggurat.messaging.rabbitmq.cluster.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.rabbitmq.cluster.producer :as rmc-prod]
            [langohr.queue :as lq]
            [langohr.exchange :as le]
            [langohr.channel :as lch]
            [langohr.http :as lh]
            [ziggurat.fixtures :as fix]
            [clojure.string :as str])
  (:import (com.rabbitmq.client Channel Connection)
           (org.apache.kafka.common.header Header)))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/silence-logging]))

(def rmq-cluster-config {:hosts "localhost-1,localhost-2,localhost-3"
                         :port 5672
                         :username "rabbit"
                         :password "rabbit"
                         :channel-timeout 2000
                         :ha-mode "all"
                         :ha-params 1
                         :ha-sync-mode "automatic"})

(def rmq-cluster-config-without-ha-config (dissoc rmq-cluster-config :ha-mode :ha-params :ha-sync-mode))

(defn- create-mock-channel [] (reify Channel
                                (close [_] nil)))

(deftest get-default-ha-policy-test
  (testing "it should ignore `:ha-params` when `:ha-mode` is `all`"
    (let [expected-ha-policy {:ha-mode "all" :ha-sync-mode "automatic"}
          ha-policy (rmc-prod/get-default-ha-policy rmq-cluster-config (count (:hosts rmq-cluster-config)))]
      (is (= ha-policy expected-ha-policy))))
  (testing "it should use the `:ha-params` if specified when `:ha-mode` is `exactly`"
    (let [expected-ha-policy                    {:ha-mode "exactly" :ha-sync-mode "automatic" :ha-params 1}
          rmq-cluster-conf-with-ha-mode-exactly (assoc rmq-cluster-config :ha-mode "exactly")
          ha-policy                             (rmc-prod/get-default-ha-policy rmq-cluster-conf-with-ha-mode-exactly (count (:hosts rmq-cluster-config)))]
      (is (= ha-policy expected-ha-policy)))))

(deftest create-and-bind-queue-test
  (testing "it should create a queue,an exchange and bind the queue to the exchange but not tag the queue with a dead-letter exchange"
    (let [default-props {:durable true :auto-delete false}
          default-props-with-arguments (assoc default-props :arguments {})
          exchange-type "fanout"
          queue-name "test-queue"
          exchange-name "test-exchange"
          ha-policy-name (str queue-name "_ha_policy")
          ha-policy-body {:apply-to "all"
                          :pattern (str "^"  queue-name "|" exchange-name "$")
                          :definition {:ha-mode (:ha-mode rmq-cluster-config)
                                       :ha-sync-mode (:ha-sync-mode rmq-cluster-config)}}
          exchange-declare-called? (atom false)
          queue-declare-called? (atom false)
          bind-called? (atom false)
          http-called? (atom false)]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String queue props]
                                 (when (and (= props default-props-with-arguments)
                                            (= queue-name queue))
                                   (reset! queue-declare-called? true)))
                    le/declare (fn [^Channel _ ^String name ^String type props]
                                 (when (and (= name exchange-name)
                                            (= props default-props)
                                            (= exchange-type type))
                                   (reset! exchange-declare-called? true)))
                    lq/bind    (fn [^Channel _ ^String queue ^String exchange]
                                 (when (and (= queue queue-name)
                                            (= exchange exchange-name))
                                   (reset! bind-called? true)))
                    lh/set-policy (fn [^String vhost ^String name policy]
                                    (when (and (= "/" vhost)
                                               (= lh/*endpoint* "http://localhost-1:15672")
                                               (= lh/*username* "rabbit")
                                               (= lh/*password* "rabbit")
                                               (= ha-policy-name name)
                                               (= policy ha-policy-body))
                                      (reset! http-called? true)))]
        (rmc-prod/create-and-bind-queue rmq-cluster-config nil queue-name exchange-name nil))
      (is (true? @bind-called?))
      (is (true? @exchange-declare-called?))
      (is (true? @http-called?))
      (is (true? @queue-declare-called?))))

  (testing "it should create a queue, an exchange, bind the queue to the exchange and tag it with dead-letter-exchange"
    (let [default-props {:durable true :auto-delete false}
          dead-letter-exchange-name "test-dead-letter-exchange"
          queue-name "test-queue"
          exchange-name "test-exchange"
          exchange-type "fanout"
          ha-policy-body {:apply-to "all"
                          :pattern (str "^" queue-name "|" exchange-name "$")
                          :definition {:ha-mode (:ha-mode rmq-cluster-config)
                                       :ha-sync-mode (:ha-sync-mode rmq-cluster-config)}}
          ha-policy-name (str queue-name "_ha_policy")
          default-props-with-arguments (assoc default-props :arguments  {"x-dead-letter-exchange" dead-letter-exchange-name})
          exchange-declare-called? (atom false)
          queue-declare-called? (atom false)
          bind-called? (atom false)
          http-called? (atom false)]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String queue props]
                                 (when (and (= props default-props-with-arguments)
                                            (= queue-name queue))
                                   (reset! queue-declare-called? true)))
                    le/declare (fn [^Channel _ ^String name ^String type props]
                                 (when (and (= name exchange-name)
                                            (= props default-props)
                                            (= type exchange-type))
                                   (reset! exchange-declare-called? true)))
                    lq/bind    (fn [^Channel _ ^String queue ^String exchange]
                                 (when (and (= queue queue-name)
                                            (= exchange exchange-name))
                                   (reset! bind-called? true)))
                    lh/set-policy (fn [^String vhost ^String name policy]
                                    (when (and (= "/" vhost)
                                               (= ha-policy-name name)
                                               (= lh/*endpoint* "http://localhost-1:15672")
                                               (= lh/*username* "rabbit")
                                               (= lh/*password* "rabbit")
                                               (= policy ha-policy-body))
                                      (reset! http-called? true)))]
        (rmc-prod/create-and-bind-queue rmq-cluster-config nil queue-name exchange-name dead-letter-exchange-name))
      (is (true? @bind-called?))
      (is (true? @exchange-declare-called?))
      (is (true? @http-called?))
      (is (true? @queue-declare-called?))))

  (testing "it should apply default ha-config when ha-config is not defined in the config"
    (let [default-props {:durable true :auto-delete false}
          dead-letter-exchange-name "test-dead-letter-exchange"
          queue-name "test-queue"
          exchange-name "test-exchange"
          exchange-type "fanout"
          ha-policy-body {:apply-to "all"
                          :pattern (str "^" queue-name "|" exchange-name "$")
                          :definition {:ha-mode "exactly"
                                       :ha-sync-mode "automatic"
                                       :ha-params 2}}
          ha-policy-name (str queue-name "_ha_policy")
          default-props-with-arguments (assoc default-props :arguments  {"x-dead-letter-exchange" dead-letter-exchange-name})
          exchange-declare-called? (atom false)
          queue-declare-called? (atom false)
          bind-called? (atom false)
          http-called? (atom false)]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String queue props]
                                 (when (and (= props default-props-with-arguments)
                                            (= queue-name queue))
                                   (reset! queue-declare-called? true)))
                    le/declare (fn [^Channel _ ^String name ^String type props]
                                 (when (and (= name exchange-name)
                                            (= props default-props)
                                            (= type exchange-type))
                                   (reset! exchange-declare-called? true)))
                    lq/bind    (fn [^Channel _ ^String queue ^String exchange]
                                 (when (and (= queue queue-name)
                                            (= exchange exchange-name))
                                   (reset! bind-called? true)))
                    lh/set-policy (fn [^String vhost ^String name policy]
                                    (when (and (= "/" vhost)
                                               (= ha-policy-name name)
                                               (= lh/*endpoint* "http://localhost-1:15672")
                                               (= lh/*username* "rabbit")
                                               (= lh/*password* "rabbit")
                                               (= policy ha-policy-body))
                                      (reset! http-called? true)))]
        (rmc-prod/create-and-bind-queue rmq-cluster-config-without-ha-config nil queue-name exchange-name dead-letter-exchange-name))
      (is (true? @bind-called?))
      (is (true? @exchange-declare-called?))
      (is (true? @http-called?))
      (is (true? @queue-declare-called?))))

  (testing "it should catch an exception when create queue raises an exception"
    (let [dead-letter-exchange-name "test-dead-letter-exchange"
          queue-name "test-queue"
          exchange-name "test-exchange"]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String _ _] (throw (Exception. "error creating a queue")))]
        (is (thrown? Exception (rmc-prod/create-and-bind-queue rmq-cluster-config nil queue-name exchange-name dead-letter-exchange-name))))))

  (testing "it should catch an exception when declare exchange raises an exception"
    (let [dead-letter-exchange-name "test-dead-letter-exchange"
          queue-name "test-queue"
          exchange-name "test-exchange"]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String _ _] nil)
                    le/declare (fn [^Channel _ ^String name ^String type props]
                                 (throw (Exception. "error declaring an exchange")))]
        (is (thrown? Exception (rmc-prod/create-and-bind-queue rmq-cluster-config nil queue-name exchange-name dead-letter-exchange-name))))))

  (testing "it should catch an exception when bind queue to exchange raises an exception"
    (let [dead-letter-exchange-name "test-dead-letter-exchange"
          queue-name "test-queue"
          exchange-name "test-exchange"]
      (with-redefs [lch/open   (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String _ _] nil)
                    le/declare (fn [^Channel _ ^String name ^String type props] nil)
                    lq/bind    (fn [^Channel _ ^String queue ^String exchange] (throw (Exception. "error binding the queue to exchange")))]
        (is (thrown? Exception (rmc-prod/create-and-bind-queue rmq-cluster-config nil queue-name exchange-name dead-letter-exchange-name)))))))

