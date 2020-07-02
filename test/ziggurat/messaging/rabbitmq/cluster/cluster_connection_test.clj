(ns ziggurat.messaging.rabbitmq.cluster.cluster-connection-test
  (:require [clojure.test :refer :all]
            [langohr.core :as rmq]
            [ziggurat.messaging.rabbitmq.cluster.connection :as rmq-cluster-conn]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :as config])
  (:import (com.rabbitmq.client Address)))

(use-fixtures :once fix/mount-only-config)

(defn- transform-address-objs-to-tuples [address-objs]
  (map #(list (.getHost %) (.getPort %)) address-objs))

(deftest transform-host-str-test
  (testing "it should take a string of comma separated hostnames and return a vector of hostnames when tracer-enabled is false"
    (let [host-str     "localhost-1,localhost-2,localhost-3"
          expected-vec ["localhost-1", "localhost-2", "localhost-3"]
          port         5672]
      (is (= expected-vec (rmq-cluster-conn/transform-host-str host-str port false)))))

  (testing "it should take a string of comma separated hostnames and return a list of `Address` objects when tracer-enabled is true"
    (let [host-str     "localhost-1,localhost-2,localhost-3"
          port         5672
          expected-list (list (list "localhost-1" port) (list "localhost-2" port) (list "localhost-3" port))]
      (let [address-objs   (rmq-cluster-conn/transform-host-str host-str port true)
            address-tuples (transform-address-objs-to-tuples address-objs)]
        (is (= expected-list address-tuples))))))

(deftest create-connection-test
  (testing "`rmq-connect` should be called with the correct arguments when tracer is disabled"
    (let [rmq-config  (get-in config/config [:ziggurat :rabbit-mq-connection])
          rmq-cluster-config (assoc rmq-config
                                    :hosts "localhost-1,localhost-2,localhost-3")
          expected-config (assoc rmq-cluster-config
                                 :hosts ["localhost-1", "localhost-2", "localhost-3"])
          rmq-connect-called? (atom false)]
      (with-redefs (rmq/connect (fn [config]
                                  (when (= config expected-config)
                                    (reset! rmq-connect-called? true))))
        (rmq-cluster-conn/create-connection rmq-cluster-config false)
        (is (true? @rmq-connect-called?)))))

  (testing "`rmq-cluster-conn` should be called with the correct arguments when tracer is enabled"
    (let [rmq-config  (get-in config/config [:ziggurat :rabbit-mq-connection])
          rmq-cluster-config (assoc rmq-config
                                    :hosts "localhost-1,localhost-2,localhost-3"
                                    :port 5672)
          expected-config (assoc rmq-cluster-config
                                 :hosts (list (Address. "localhost-1" 5672) (Address. "localhost-2" 5672) (Address. "localhost-3" 5672))
                                 :port 5672)
          rmq-connect-called? (atom false)]
      (with-redefs (rmq-cluster-conn/create-traced-clustered-connection (fn [config]
                                                                          (when (and (= (transform-address-objs-to-tuples (:hosts config)) (transform-address-objs-to-tuples (:hosts expected-config)))
                                                                                     (= (dissoc (:hosts config)) (dissoc (:hosts expected-config))))
                                                                            (reset! rmq-connect-called? true))))
        (rmq-cluster-conn/create-connection rmq-cluster-config true)
        (is (true? @rmq-connect-called?))))))