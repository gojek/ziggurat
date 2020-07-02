(ns ziggurat.messaging.rabbitmq.connection-test
  (:require [clojure.test :refer :all]
            [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [langohr.core :as rmq]
            [ziggurat.config :as config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.connection :as rmq-conn]))

(use-fixtures :once fix/mount-config-with-tracer)

(deftest ^:integration start-connection-test-with-tracer-disabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count             (atom 0)
          orig-rmq-connect         rmq/connect
          rmq-connect-called?      (atom false)
          stream-routes            {:default {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}}
          overriden-default-config (assoc config/config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :jobs {:instant {:worker-count 4}}
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}
                                                           :tracer {}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! rmq-connect-called? true)
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-default-config]
        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 14))
        (is @rmq-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count             (atom 0)
          orig-rmq-connect         rmq/connect
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :success)}}
          overriden-default-config (assoc default-config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :jobs {:instant {:worker-count 4}}
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                :channel-2 {:worker-count 10}}}}
                                                           :tracer {:enabled false}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-default-config]
        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          default-config   config/config
          overriden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                                  :jobs {:instant {:worker-count 4}}
                                                                  :stream-router {:default {}}
                                                                  :tracer {:enabled false}))
          stream-routes    {:default {}}]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-config]

        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-rmq-connect  rmq/connect
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config) :jobs {:instant {:worker-count 4}}
                                                                   :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                                   :default-1 {:channels {:channel-1 {:worker-count 8}}}}
                                                                   :tracer {:enabled false}))
          stream-routes     {}]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overridden-config]
        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 26))))))

(deftest ^:integration start-connection-test-with-tracer-enabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count             (atom 0)
          orig-create-conn         rmq-conn/create-connection
          create-connect-called?   (atom false)
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}}
          overriden-default-config (assoc default-config
                                          :ziggurat (assoc (ziggurat-config)
                                                           :jobs {:instant {:worker-count 4}}
                                                           :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}))]
      (with-redefs [rmq-conn/create-connection (fn [provided-config tracer-enabled]
                                                 (reset! create-connect-called? true)
                                                 (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                 (orig-create-conn provided-config tracer-enabled))
                    config/config              overriden-default-config]
        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 14))
        (is @create-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count      (atom 0)
          orig-create-conn  rmq-conn/create-connection
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                                   :jobs {:instant {:worker-count 4}}
                                                                   :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                        :channel-2 {:worker-count 10}}}}))
          stream-routes     {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq-conn/create-connection (fn [provided-config tracer-enabled]
                                                 (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                 (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-config]
        (rmqw/start-connection config/config stream-routes)
        (rmqw/stop-connection config/config stream-routes)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count              (atom 0)
          orig-create-conn          rmq-conn/create-connection
          default-config            config/config
          overridden-default-config (assoc default-config
                                           :ziggurat (assoc (ziggurat-config)
                                                            :jobs {:instant {:worker-count 4}}
                                                            :stream-router {:default {}}))]
      (with-redefs [rmq-conn/create-connection (fn [provided-config tracer-enabled]
                                                 (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                 (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-default-config]

        (rmqw/start-connection config/config {})
        (rmqw/stop-connection config/config {})
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-create-conn  rmq-conn/create-connection
          default-config    config/config
          overridden-config (assoc default-config
                                   :ziggurat (assoc (ziggurat-config)
                                                    :jobs {:instant {:worker-count 4}}
                                                    :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                    :default-1 {:channels {:channel-1 {:worker-count 8}}}}))]
      (with-redefs [rmq-conn/create-connection (fn [provided-config tracer-enabled]
                                                 (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                                 (orig-create-conn provided-config tracer-enabled))
                    config/config              overridden-config]

        (rmqw/start-connection config/config {})
        (rmqw/stop-connection config/config {})
        (is (= @thread-count 26))))))

