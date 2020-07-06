(ns ziggurat.messaging.rabbitmq.cluster.connection-test
  (:require [clojure.test :refer :all]
            [langohr.core :as rmq]
            [ziggurat.messaging.rabbitmq.cluster.connection :as rmq-cluster-conn]
            [ziggurat.messaging.rabbitmq-cluster-wrapper :as rmqcw]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :as config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq.connection :as rmq-conn])
  (:import (com.rabbitmq.client Address)))

(use-fixtures :once  fix/mount-config-with-tracer)

(def rmq-cluster-config-for-integration-tests {:hosts "localhost" :port 5672 :username "guest" :password "guest" :channel-timeout 2000})

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

(deftest ^:integration start-connection-test-with-tracer-disabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count             (atom 0)
          orig-rmq-connect         rmq/connect
          rmq-connect-called?      (atom false)
          stream-routes            {:default {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}}
          overriden-default-config (assoc config/config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                           :jobs {:instant {:worker-count 4}}
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}
                                                           :tracer {}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! rmq-connect-called? true)
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-default-config]
        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 14))
        (is @rmq-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count             (atom 0)
          orig-rmq-connect         rmq/connect
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :success)}}
          overriden-default-config (assoc default-config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                           :jobs {:instant {:worker-count 4}}
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                :channel-2 {:worker-count 10}}}}
                                                           :tracer {:enabled false}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-default-config]
        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          default-config   config/config
          overriden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                                  :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                                  :jobs {:instant {:worker-count 4}}
                                                                  :stream-router {:default {}}
                                                                  :tracer {:enabled false}))
          stream-routes    {:default {}}]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-config]

        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-rmq-connect  rmq/connect
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                                   :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                                   :jobs {:instant {:worker-count 4}}
                                                                   :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                                   :default-1 {:channels {:channel-1 {:worker-count 8}}}}
                                                                   :tracer {:enabled false}))
          stream-routes     {}]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overridden-config]
        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 26))))))

(deftest ^:integration start-connection-test-with-tracer-enabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count             (atom 0)
          orig-create-conn         rmq-cluster-conn/create-connection
          create-connect-called?   (atom false)
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}}
          overriden-default-config (assoc default-config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :jobs {:instant {:worker-count 4}}
                                                           :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}))]
      (with-redefs [rmq-cluster-conn/create-connection (fn [provided-config tracer-enabled]
                                                         (reset! create-connect-called? true)
                                                         (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                         (orig-create-conn provided-config tracer-enabled))
                    config/config              overriden-default-config]
        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 14))
        (is @create-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count      (atom 0)
          orig-create-conn  rmq-cluster-conn/create-connection
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                                   :jobs {:instant {:worker-count 4}}
                                                                   :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                                   :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                        :channel-2 {:worker-count 10}}}}))
          stream-routes     {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq-cluster-conn/create-connection (fn [provided-config tracer-enabled]
                                                         (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                         (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-config]
        (rmqcw/start-connection config/config stream-routes)
        (rmqcw/stop-connection config/config stream-routes)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count              (atom 0)
          orig-create-conn          rmq-cluster-conn/create-connection
          default-config            config/config
          overridden-default-config (assoc default-config
                                           :ziggurat (assoc (ziggurat-config)
                                                            :jobs {:instant {:worker-count 4}}
                                                            :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                            :stream-router {:default {}}))]
      (with-redefs [rmq-cluster-conn/create-connection (fn [provided-config tracer-enabled]
                                                         (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                         (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-default-config]

        (rmqcw/start-connection config/config {})
        (rmqcw/stop-connection config/config {})
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-create-conn  rmq-cluster-conn/create-connection
          default-config    config/config
          overridden-config (assoc default-config
                                   :ziggurat (assoc (ziggurat-config)
                                                    :rabbit-mq-connection rmq-cluster-config-for-integration-tests
                                                    :jobs {:instant {:worker-count 4}}
                                                    :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                    :default-1 {:channels {:channel-1 {:worker-count 8}}}}))]
      (with-redefs [rmq-cluster-conn/create-connection (fn [provided-config tracer-enabled]
                                                         (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                         (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-config]

        (rmqcw/start-connection config/config {})
        (rmqcw/stop-connection config/config {})
        (is (= @thread-count 26))))))
