(ns ziggurat.messaging.channel-pool-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.channel_pool :as cpool :refer [channel-pool]]
            [ziggurat.messaging.connection :refer [producer-connection]]
            [ziggurat.fixtures :as fix])
  (:import (org.apache.commons.pool2.impl GenericObjectPoolConfig GenericObjectPool)
           (java.time Duration)
           (com.rabbitmq.client Channel)))

(use-fixtures :once (join-fixtures [fix/mount-only-config
                                    fix/mount-config-with-tracer]))

(deftest calc-total-threads-test
  (testing "it should calculate the total threads configured for RabbitMQ, Kafka streams and Batch consumers"
    (let [expected-count 44
          actual-count   (cpool/calc-total-thread-count)]
      (is (= expected-count actual-count)))))

(deftest create-object-pool-config-test
  (testing "it should create a PoolConfig with default values"
    (let [expected-config    {:min-idle 10 :max-idle 44 :max-total 54 :max-wait-ms 5000}
          pool-config-object ^GenericObjectPoolConfig (cpool/create-object-pool-config {})
          min-idle           (.getMinIdle pool-config-object)
          max-idle           (.getMaxIdle pool-config-object)
          max-wait-ms        (.getMaxWaitDuration pool-config-object)
          test-on-borrow     (.getTestOnBorrow pool-config-object)
          max-total          (.getMaxTotal pool-config-object)]
      (is (= (:min-idle expected-config) min-idle))
      (is (= (:max-idle expected-config) max-idle))
      (is test-on-borrow)
      (is (= (Duration/ofMillis (:max-wait-ms expected-config)) max-wait-ms))
      (is (= (:max-total expected-config) max-total))))
  (testing "it should override the default config with the user provided config"
    (let [expected-config    {:min-idle 5 :max-idle 200 :max-total 49 :max-wait-ms 1000}
          user-config        {:min-idle 5 :max-idle 200 :max-wait-ms 1000}
          pool-config-object ^GenericObjectPoolConfig (cpool/create-object-pool-config user-config)
          min-idle           (.getMinIdle pool-config-object)
          max-idle           (.getMaxIdle pool-config-object)
          test-on-borrow     (.getTestOnBorrow pool-config-object)
          max-total          (.getMaxTotal pool-config-object)
          max-wait-ms        (.getMaxWaitDuration pool-config-object)]
      (is (= (:min-idle expected-config) min-idle))
      (is (= (:max-idle expected-config) max-idle))
      (is (= (Duration/ofMillis (:max-wait-ms expected-config)) max-wait-ms))
      (is test-on-borrow)
      (is (= (:max-total expected-config) max-total))))
  (testing "if min idle is greater than max idle it replaces max idle with min idle"
    (let [expected-config    {:min-idle 15 :max-idle 15 :max-total 59 :max-wait-ms 1000}
          user-config        {:min-idle 15 :max-idle 2 :max-wait-ms 1000}
          pool-config-object ^GenericObjectPoolConfig (cpool/create-object-pool-config user-config)
          min-idle           (.getMinIdle pool-config-object)
          max-idle           (.getMaxIdle pool-config-object)
          test-on-borrow     (.getTestOnBorrow pool-config-object)
          max-total          (.getMaxTotal pool-config-object)
          max-wait-ms        (.getMaxWaitDuration pool-config-object)]
      (is (= (:min-idle expected-config) min-idle))
      (is (= (:max-idle expected-config) max-idle))
      (is (= (Duration/ofMillis (:max-wait-ms expected-config)) max-wait-ms))
      (is test-on-borrow)
      (is (= (:max-total expected-config) max-total)))))

(deftest pool-borrow-return-test
  (testing "it should invalidate a closed channel and return a new channel on borrow"
    (mount.core/start #'producer-connection)
    (let [channel-pool ^GenericObjectPool (cpool/create-channel-pool producer-connection)
          _            (doto channel-pool
                         (.setMaxTotal 1)
                         (.setMinIdle 0)
                         (.setMaxIdle 1))
          rmq-chan     ^Channel (.borrowObject channel-pool)
          _            (.close rmq-chan)
          _            (.returnObject channel-pool rmq-chan)
          rmq-chan-2   ^Channel (.borrowObject channel-pool)]
      (is (not (.equals rmq-chan-2 rmq-chan)))
      (is (.isOpen rmq-chan-2)))))

(deftest is-pool-alive?
  (testing "it should return true when channel-pool state has started"
    (let [states {#'producer-connection #'channel-pool}]
      (mount.core/start states)
      (is (cpool/is-pool-alive? channel-pool))
      (mount.core/stop states)))
  (testing "it should return false when channel-pool state has not been started"
    (is (false? (cpool/is-pool-alive? channel-pool))))
  (testing "it should return false when channel-pool state has been stopped"
    (let [states {#'producer-connection #'channel-pool}]
      (mount.core/start states)
      (mount.core/stop states)
      (is (false? (cpool/is-pool-alive? channel-pool))))))
