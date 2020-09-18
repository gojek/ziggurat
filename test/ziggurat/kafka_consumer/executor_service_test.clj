(ns ziggurat.kafka-consumer.executor-service-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config] :as config]
            [ziggurat.kafka-consumer.executor-service :refer :all]
            [ziggurat.fixtures :as fix]
            [mount.core :as mount])
  (:import (java.util.concurrent ThreadPoolExecutor)))

(use-fixtures :once fix/mount-only-config)

(deftest thread-pool-test
  (testing "should create a thread-pool with the total number of threads in the configuration"
    (let [expected-thread-count 6]
      (-> (mount/only [#'thread-pool])
          (mount/start))
      (is (= expected-thread-count (.getMaximumPoolSize (cast ThreadPoolExecutor thread-pool))))
      (-> (mount/only [#'thread-pool])
          (mount/stop)))))

(deftest thread-pool-test-with-default-thread-count
  (testing "should create a thread-pool using DEFAULT_THREAD_COUNT when thread count is not provided in the configuration"
    (with-redefs [config/config (->
                                  (assoc-in config/config [:ziggurat :batch-routes :consumer-1 :thread-count] nil)
                                  (assoc-in [:ziggurat :batch-routes :consumer-2 :thread-count] nil))]
      (let [expected-thread-count 4]
      (-> (mount/only [#'thread-pool])
          (mount/start))
      (is (= expected-thread-count (.getMaximumPoolSize (cast ThreadPoolExecutor thread-pool))))
      (-> (mount/only [#'thread-pool])
          (mount/stop))))))
