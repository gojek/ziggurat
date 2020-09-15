(ns ziggurat.kafka-consumer.executor-thread-pool-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.kafka-consumer.executor-thread-pool :refer :all]
            [ziggurat.fixtures :as fix]
            [mount.core :as mount])
  (:import (java.util.concurrent ThreadPoolExecutor)))

(use-fixtures :once fix/mount-only-config)

(deftest thread-pool-test
  (testing "should create a thread-pool with the total number of threads in the configuration"
    (let [expected-thread-count 6]
      (-> (mount/only [#'executor-thread-pool])
          (mount/start))
      (is (= expected-thread-count (.getMaximumPoolSize (cast ThreadPoolExecutor executor-thread-pool))))
      (-> (mount/only [#'executor-thread-pool])
          (mount/stop)))))

