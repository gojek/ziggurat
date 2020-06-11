(ns ziggurat.messaging.rabbitmq-wrapper-test
  (:require [clojure.test :refer :all]
            [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [langohr.core :as rmq]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]))

(use-fixtures :once fix/mount-config-with-tracer)

(deftest start-connection-test-with-tracer-disabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count        (atom 0)
          orig-rmq-connect    rmq/connect
          rmq-connect-called? (atom false)
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :channel-1)
                                         :channel-1  (constantly :success)}}]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}
                                                              :tracer {}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'connection)
        (is (= @thread-count 14))
        (is @rmq-connect-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    rmq/connect
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @rmq-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    rmq/connect
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (-> ziggurat-config
                                                           (assoc :retry {:enabled false})
                                                           (dissoc :tracer)))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (not @rmq-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    rmq/connect
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default   {:handler-fn (constantly :channel-1)
                                           :channel-1  (constantly :success)}
                               :default-1 {:handler-fn (constantly :channel-3)
                                           :channel-3  (constantly :success)}}]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! rmq-connect-called? true)
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @rmq-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)
          stream-routes    {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                   :channel-2 {:worker-count 10}}}}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {}}
                                                              :tracer {:enabled false}))]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                              :default-1 {:channels {:channel-1 {:worker-count 8}}}}
                                                              :tracer {:enabled false}))]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 26))))))

(deftest start-connection-test-with-tracer-enabled
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count           (atom 0)
          orig-create-conn       rmqw/create-connection
          create-connect-called? (atom false)
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default {:handler-fn (constantly :channel-1)
                                            :channel-1  (constantly :success)}}]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 14))
        (is @create-connect-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @create-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (not @create-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       rmqw/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default   {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}
                                  :default-1 {:handler-fn (constantly :channel-3)
                                              :channel-3  (constantly :success)}}]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is @create-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count     (atom 0)
          orig-create-conn mc/create-connection
          ziggurat-config  (config/ziggurat-config)
          stream-routes    {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                                   :channel-2 {:worker-count 10}}}}))]
        (-> (mount/only #{#'rmqw/connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-create-conn rmqw/create-connection
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {}}))]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count     (atom 0)
          orig-create-conn rmqw/create-connection
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmqw/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                              :default-1 {:channels {:channel-1 {:worker-count 8}}}}))]
        (mount/start (mount/only [#'rmqw/connection]))
        (mount/stop #'rmqw/connection)
        (is (= @thread-count 26))))))