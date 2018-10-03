(ns ziggurat.messaging.connection-test
  (:require [clojure.test :refer :all])
  (:require         [ziggurat.fixtures :as fix]
                    [langohr.core :as rmq]
                    [mount.core :as mount]
                    [ziggurat.config :as config]
                    [ziggurat.messaging.connection :refer [connection]]))

(use-fixtures :once fix/mount-only-config)

(deftest start-connection-test
  (testing "should provide the correct number of threads for the thread pool if channels are present"
    (let [thread-count (atom 0)
          orig-rmq-connect rmq/connect]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly {:jobs                 {:instant {:worker-count   4}}
                                                        :retry                {:enabled [true :bool]}
                                                        :stream-router        {:booking {:channels  {:channel-1 {:worker-count 10}}}}})]
        (mount/start (mount/only [#'connection]))
        (mount/stop)
        (is (= @thread-count 14)))))

  (testing "should provide the correct number of threads for the thread pool for multiple channels"
    (let [thread-count (atom 0)
          orig-rmq-connect rmq/connect]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly {:jobs                 {:instant {:worker-count   4}}
                                                        :retry                {:enabled [true :bool]}
                                                        :stream-router        {:booking {:channels  {:channel-1 {:worker-count 5}
                                                                                                     :channel-2 {:worker-count 10}}}}})]
        (mount/start (mount/only [#'connection]))
        (mount/stop)
        (is (= @thread-count 19)))))

  (testing "should provide the correct number of threads for the thread pool when channels are not present"
    (let [thread-count (atom 0)
          orig-rmq-connect rmq/connect]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly {:jobs                 {:instant {:worker-count   4}}
                                                        :retry                {:enabled true}
                                                        :stream-router        {:booking {}}})]
        (mount/start (mount/only [#'connection]))
        (mount/stop)
        (is (= @thread-count 4)))))

  (testing "should provide the correct number of threads for the thread pool for multiple stream routes"
    (let [thread-count (atom 0)
          orig-rmq-connect rmq/connect]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))
                    config/ziggurat-config (constantly {:jobs                 {:instant {:worker-count   4}}
                                                        :retry                {:enabled [true :bool]}
                                                        :stream-router        {:booking {:channels  {:channel-1 {:worker-count 10}}}
                                                                               :driver {:channels  {:channel-1 {:worker-count 8}}}}})]
        (mount/start (mount/only [#'connection]))
        (mount/stop)
        (is (= @thread-count 26))))))