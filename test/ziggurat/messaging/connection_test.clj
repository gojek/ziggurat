(ns ziggurat.messaging.connection-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [langohr.core :as rmq]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.connection :refer [connection]]))

(use-fixtures :once fix/mount-only-config)

(deftest start-connection-test
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count        (atom 0)
          orig-rmq-connect    rmq/connect
          rmq-connect-called? (atom false)
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :channel-1)
                                         :channel-1  (constantly :success)}}]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! rmq-connect-called? true)
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :jobs {:instant {:worker-count 4}}
                                                         :retry {:enabled true}
                                                         :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}))]
        (-> (mount/only #{#'connection})
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
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! rmq-connect-called? true)
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :retry {:enabled true}))]
        (-> (mount/only #{#'connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'connection)
        (is @rmq-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    rmq/connect
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! rmq-connect-called? true)
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :retry {:enabled false}))]
        (-> (mount/only #{#'connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'connection)
        (is (not @rmq-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    rmq/connect
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default   {:handler-fn (constantly :channel-1)
                                           :channel-1  (constantly :success)}
                               :default-1 {:handler-fn (constantly :channel-3)
                                           :channel-3  (constantly :success)}}]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! rmq-connect-called? true)
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :retry {:enabled false}))]
        (-> (mount/only #{#'connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'connection)
        (is @rmq-connect-called?))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)
          stream-routes    {:default {:handler-fn (constantly :success)}}]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :jobs {:instant {:worker-count 4}}
                                                         :retry {:enabled true}
                                                         :stream-router {:default {:channels {:channel-1 {:worker-count 5}
                                                                                              :channel-2 {:worker-count 10}}}}))]
        (-> (mount/only #{#'connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'connection)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :jobs {:instant {:worker-count 4}}
                                                         :retry {:enabled true}
                                                         :stream-router {:default {}}))]
        (mount/start (mount/only [#'connection]))
        (mount/stop #'connection)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count     (atom 0)
          orig-rmq-connect rmq/connect
          ziggurat-config  (config/ziggurat-config)]
      (with-redefs [rmq/connect            (fn [provided-config]
                                             (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                             (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                         :jobs {:instant {:worker-count 4}}
                                                         :retry {:enabled true}
                                                         :stream-router {:default   {:channels {:channel-1 {:worker-count 10}}}
                                                                         :default-1 {:channels {:channel-1 {:worker-count 8}}}}))]
        (mount/start (mount/only [#'connection]))
        (mount/stop #'connection)
        (is (= @thread-count 26))))))