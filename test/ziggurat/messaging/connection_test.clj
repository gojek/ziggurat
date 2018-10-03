(ns ziggurat.messaging.connection-test
  (:require [clojure.test :refer :all])
  (:require         [ziggurat.fixtures :as fix]
                    [langohr.core :as rmq]
                    [mount.core :as mount]
                    [ziggurat.config :as config]
                    [ziggurat.messaging.connection :refer [connection]]))

(use-fixtures :once fix/mount-only-config)

(deftest retry-test
  (testing "should provide the correct number of thread for the thread pool"
    (let [thread-count (atom 0)
          orig-rmq-connect rmq/connect]
      (with-redefs [rmq/connect (fn [provided-config]
                                  (reset! thread-count (.getCorePoolSize (:executor provided-config)))
                                  (orig-rmq-connect provided-config))]
        (mount/start (mount/only [#'connection]))
        (is (= @thread-count 14))))))