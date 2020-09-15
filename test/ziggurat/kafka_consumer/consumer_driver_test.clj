(ns ziggurat.kafka-consumer.consumer-driver-test
  (:require [clojure.test :refer :all]
            [ziggurat.kafka-consumer.consumer :as ct]
            [ziggurat.kafka-consumer.consumer-handler :as ch]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.kafka-consumer.consumer-driver :as cd :refer [consumer-groups]]
            [mount.core :as mount]
            [ziggurat.kafka-consumer.executor-thread-pool :refer [executor-thread-pool]]
            [ziggurat.fixtures :as fix]
            [clojure.tools.logging :as log]
            [ziggurat.metrics :as metrics])
  (:import (java.util.concurrent ExecutorService RejectedExecutionHandler)
           (org.apache.kafka.clients.consumer Consumer)))

(use-fixtures :each (join-fixtures [fix/mount-only-config fix/mount-test-thread-pool]))

(defn- dummy-handler-fn [batch-message]
  :dummy-val)

(deftest create-consumers-per-group-test
  (testing "should create the required number of consumers and consumer groups as per the config in config.edn"
    (let [stopped-consumer-groups-count  (atom 0)
          stopped-consumer-count         (atom 0)]
      (with-redefs [ct/create-consumer   (fn [consumer-config]
                                           (is (not (nil? consumer-config)))
                                           ;; returning a dummy data instead of a consumer
                                           {:dummy-key :dummy-value})
                    ch/poll-for-messages (fn [consumer handler-fn consumer-id consumer-config]
                                           (do
                                             (is (= consumer {:dummy-key :dummy-value}))
                                             (is (= :dummy-val (handler-fn [])))
                                             (is (not (nil? consumer-config)))))
                    cd/stop-consumers        (fn [consumer-groups]
                                               (doseq [[consumer-group-id consumers] consumer-groups]
                                                 (swap! stopped-consumer-groups-count inc)
                                                 (doseq [consumer consumers]
                                                   (swap! stopped-consumer-count inc))))]
        (-> (mount/only [#'consumer-groups])
            (mount/with-args {:consumer-1 {:handler-fn dummy-handler-fn}
                              :consumer-2 {:handler-fn dummy-handler-fn}})
            (mount/start))
        (is (= 2 (count consumer-groups)))
        (is (= 2 (count (:consumer-1 consumer-groups))))
        (is (= 4 (count (:consumer-2 consumer-groups))))
        (-> (mount/only [#'consumer-groups])
            (mount/stop))
        (is (= @stopped-consumer-count 6))
        (is (= @stopped-consumer-groups-count 2)))))
  (testing "should not start polling if consumer is not created, i.e. consumer is nil"
    (let [consumer-nil-polling-started                (atom false)]
      (with-redefs [ct/create-consumer   (constantly nil)
                    ch/poll-for-messages (fn [_ _ _ _]
                                           (reset! consumer-nil-polling-started true))
                    cd/stop-consumers        (constantly nil)] ;; stop-consumers does nothing as there are no consumers
        (-> (mount/only [#'consumer-groups])
            (mount/with-args {:consumer-1 {:handler-fn dummy-handler-fn}
                              :consumer-2 {:handler-fn dummy-handler-fn}})
            (mount/start))
        (is (= false @consumer-nil-polling-started))
        (-> (mount/only [#'consumer-groups])
            (mount/stop)))))
  (testing "should not submit a task to the thread-pool if the task is nil"
    (let [polling-started                (atom false)]
      (with-redefs [ct/create-consumer   (fn [consumer-config]
                                           (is (not (nil? consumer-config)))
                                           ;; returning a dummy data instead of a consumer
                                           {:dummy-key :dummy-value})
                    ch/poll-for-messages (fn [_ _ _ _]
                                           (reset! polling-started true))
                    cd/stop-consumers       (constantly nil) ;; stop-consumers does nothing as there are just dummy consumers
                    cast                 (constantly nil)]
        (-> (mount/only [#'consumer-groups])
            (mount/with-args {:consumer-1 {:handler-fn dummy-handler-fn}
                              :consumer-2 {:handler-fn dummy-handler-fn}})
            (mount/start))
        (is (= false @polling-started))
        (-> (mount/only [#'consumer-groups])
            (mount/stop))))))

(deftest task-submission-error-handling-test
  (testing "should log an error when a rejected exception is thrown by the thread-pool"
    (let [polling-started                (atom false)
          zig-config                     (ziggurat-config)]
      (with-redefs [ziggurat-config      (constantly (assoc zig-config :batch-routes {:dummy-consumer-group-1 {:thread-count 1}}))
                    ct/create-consumer   (fn [consumer-config]
                                           (is (not (nil? consumer-config)))
                                           ;; returning a dummy data instead of a consumer
                                           {:dummy-key :dummy-value})
                    ch/poll-for-messages (fn [_ _ _ _]
                                           (reset! polling-started true))
                    cd/stop-consumers       (constantly nil) ;; stop-consumers does nothing as there are just dummy consumers
                    log/error              (fn [str e]
                                             (is (= "message polling task was rejected by the threadpool" str))
                                             (is (= RejectedExecutionHandler (class e))))
                    metrics/increment-count (fn [metric-namespace metrics val _]
                                              (is (= metric-namespace ["ziggurat.batch.consumption"]))
                                              (is (= metrics "thread-pool.task.rejected"))
                                              (is (>= val 0)))]
        (.shutdown ^ExecutorService executor-thread-pool)
        (-> (mount/only [#'consumer-groups])
            (mount/with-args {:consumer-1 {:handler-fn dummy-handler-fn}})
            (mount/start))
        (is (= false @polling-started))
        (-> (mount/only [#'consumer-groups])
            (mount/stop))))))

(deftest consumer-driver-shut-down-test
  (testing "stop-consumers should call .wakeup on all consumers when shutting down"
    (let [consumers-shut-down            (atom 0)
          zig-config                     (ziggurat-config)]
      (with-redefs [ziggurat-config      (constantly (assoc zig-config :batch-routes {:dummy-consumer-group-1 {:thread-count 2}}))
                    ct/create-consumer   (fn [consumer-config]
                                           (reify Consumer
                                             (wakeup [_]
                                               (swap! consumers-shut-down inc))))
                    ch/poll-for-messages (constantly nil)]
        (-> (mount/only [#'consumer-groups])
            (mount/with-args {})
            (mount/start))
        (-> (mount/only [#'consumer-groups])
            (mount/stop))
        (is (= 2 @consumers-shut-down))))))

