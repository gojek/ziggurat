(ns ziggurat.messaging.rabbitmq-wrapper-connection-test
  (:require [clojure.test :refer :all]
            [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [langohr.core :as rmq]
            [mount.core :as mount]
            [ziggurat.config :as config :refer [ziggurat-config]]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]))

(use-fixtures :once fix/mount-config-with-tracer)

(deftest start-connection-test-with-tracer-disabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count        (atom 0)
          orig-rmq-connect    rmq/connect
          rmq-connect-called? (atom false)
          stream-routes       {:default {:handler-fn (constantly :channel-1)
                                         :channel-1  (constantly :success)}}
          overriden-default-config (assoc config/config
                                     :ziggurat (assoc (ziggurat-config)
                                                 :jobs {:instant {:worker-count 4}}
                                                 :retry {:enabled true}
                                                 :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}
                                                 :tracer {}))]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/config  overriden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 14))
        (is @rmq-connect-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [rmq-connect-called?       (atom false)
          orig-rmq-connect          rmq/connect
          stream-routes             {:default {:handler-fn (constantly :success)}}
          overridden-default-config (assoc config/config
                                      :ziggurat (assoc (ziggurat-config)
                                                  :retry {:enabled true}
                                                  :tracer {:enabled false}))]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (orig-rmq-connect provided-config))
                    config/config overridden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @rmq-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [rmq-connect-called?       (atom false)
          orig-rmq-connect          rmq/connect
          stream-routes             {:default {:handler-fn (constantly :success)}}
          overridden-default-config (assoc config/default-config
                                      :ziggurat (-> (ziggurat-config)
                                                    (assoc :retry {:enabled false})
                                                    (dissoc :tracer)))]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (orig-rmq-connect provided-config))
                    config/config overridden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (not @rmq-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [rmq-connect-called?       (atom false)
          orig-rmq-connect          rmq/connect
          stream-routes             {:default   {:handler-fn (constantly :channel-1)
                                                 :channel-1  (constantly :success)}
                                     :default-1 {:handler-fn (constantly :channel-3)
                                                 :channel-3  (constantly :success)}}
          overridden-default-config (assoc config/config
                                      :ziggurat (assoc (ziggurat-config)
                                                  :retry {:enabled false}
                                                  :tracer {:enabled false}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! rmq-connect-called? true)
                                    (orig-rmq-connect provided-config))
                    config/config overridden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @rmq-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count             (atom 0)
          orig-rmq-connect         rmq/connect
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :success)}}
          overriden-default-config (assoc default-config
                                     :ziggurat (assoc (ziggurat-config)
                                                 :jobs {:instant {:worker-count 4}}
                                                 :retry {:enabled true}
                                                 :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                      :channel-2 {:worker-count 10}}}}
                                                 :tracer {:enabled false}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          default-config   config/config
          overriden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                             :jobs {:instant {:worker-count 4}}
                                                             :retry {:enabled true}
                                                             :stream-router {:default {}}
                                                             :tracer {:enabled false}))]
      (with-redefs [rmq/connect   (fn [provided-config]
                                    (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                    (orig-rmq-connect provided-config))
                    config/config overriden-config]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-rmq-connect  rmq/connect
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config) :jobs {:instant {:worker-count 4}}
                                                                                     :retry {:enabled true}
                                                                                     :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                                                     :default-1 {:channels {:channel-1 {:worker-count 8}}}}
                                                                                     :tracer {:enabled false}))]
      (with-redefs [rmq/connect       (fn [provided-config]
                                        (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                        (orig-rmq-connect provided-config))
                    config/config overridden-config]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 26))))))

(deftest start-connection-test-with-tracer-enabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count             (atom 0)
          orig-create-conn         rmqw/create-connection
          create-connect-called?   (atom false)
          default-config           config/config
          stream-routes            {:default {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}}
          overriden-default-config (assoc default-config
                                     :ziggurat (assoc (ziggurat-config)
                                                 :jobs {:instant {:worker-count 4}}
                                                 :retry {:enabled true}
                                                 :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}))]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overriden-default-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 14))
        (is @create-connect-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          stream-routes          {:default {:handler-fn (constantly :success)}}
          overridden-config      (assoc config/config
                                   :ziggurat (assoc (ziggurat-config)
                                               :retry {:enabled true}))]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overridden-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @create-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          default-config         config/config
          overriden-config       (assoc default-config
                                   :ziggurat (assoc (ziggurat-config)
                                               :retry {:enabled false}))
          stream-routes          {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config           overriden-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (not @create-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          stream-routes          {:default   {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}
                                  :default-1 {:handler-fn (constantly :channel-3)
                                              :channel-3  (constantly :success)}}
          overridden-config      (assoc config/config
                                   :ziggurat (assoc (ziggurat-config)
                                               :retry {:enabled false}))]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overridden-config]

        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @create-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count      (atom 0)
          orig-create-conn  rmqw/create-connection
          default-config    config/config
          overridden-config (assoc default-config :ziggurat (assoc (ziggurat-config)
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                   :channel-2 {:worker-count 10}}}}))
          stream-routes     {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overridden-config]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count              (atom 0)
          orig-create-conn          rmqw/create-connection
          default-config            config/config
          overridden-default-config (assoc default-config
                                      :ziggurat (assoc (ziggurat-config)
                                                  :jobs {:instant {:worker-count 4}}
                                                  :retry {:enabled true}
                                                  :stream-router {:default {}}))]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overridden-default-config]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count      (atom 0)
          orig-create-conn  rmqw/create-connection
          default-config    config/config
          overridden-config (assoc default-config
                              :ziggurat (assoc (ziggurat-config)
                                          :jobs {:instant {:worker-count 4}}
                                          :retry {:enabled true}
                                          :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                          :default-1 {:channels {:channel-1 {:worker-count 8}}}}))]
      (with-redefs [rmqw/create-connection (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/config          overridden-config]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 26))))))

