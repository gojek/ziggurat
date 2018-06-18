(ns ziggurat.init-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.init :as init]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.streams :as streams]
            [ziggurat.server.test-utils :as tu]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [clojure.tools.logging :as log]))

(deftest start-calls-actor-start-fn-test
  (testing "The actor start fn starts after the lambda internal state and can read config"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #(deliver retry-count (-> (config/ziggurat-config) :retry :count)) [] [])
        (init/stop #())
        (is (= 5 (deref retry-count 10000 ::failure)))))))

(deftest stop-calls-actor-stop-fn-test
  (testing "The actor stop fn is called before stopping the lambda internal state"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #() {:booking {:handler-fn #(constantly nil)}} [])
        (init/stop #(deliver retry-count (-> (config/ziggurat-config) :retry :count)))
        (is (= 5 (deref retry-count 10000 ::failure)))))))

(deftest start-calls-make-queues-test
  (testing "Start calls make queues"
    (let [make-queues-called     (atom false)
          expected-stream-routes [{:default {:handler-fn #()}}]]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-producer/make-queues       (fn [stream-routes]
                                                           (swap! make-queues-called not)
                                                           (is (= stream-routes expected-stream-routes)))
                    messaging-consumer/start-subscribers (constantly nil)
                    config/config-file                   "config.test.edn"]
        (init/start #() expected-stream-routes [])
        (init/stop #())
        (is @make-queues-called)))))

(deftest start-calls-start-subscribers-test
  (testing "Start calls start subscribers"
    (let [start-subscriber-called (atom false)
          expected-stream-routes  {:default {:handler-fn #()}}]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-consumer/start-subscribers (fn [stream-routes]
                                                           (swap! start-subscriber-called not)
                                                           (is (= stream-routes expected-stream-routes)))
                    messaging-producer/make-queues       (constantly nil)
                    config/config-file                   "config.test.edn"]
        (init/start #() expected-stream-routes [])
        (init/stop #())
        (is @start-subscriber-called)))))

(deftest main-test
  (testing "Main function should call start"
    (let [start-was-called       (atom false)
          expected-stream-routes {:default {:handler-fn #(constantly nil)}}]
      (with-redefs [init/add-shutdown-hook (fn [_] (constantly nil))
                    init/start             (fn [_ stream-router _]
                                             (swap! start-was-called not)
                                             (is (= expected-stream-routes stream-router)))]
        (init/main #() #() expected-stream-routes)
        (is @start-was-called)))))

(deftest validate-stream-routes-test
  (let [exception-message "Invalid stream routes"]
    (testing "Validate Stream Routes should raise exception if stream routes is nil"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes nil))))

    (testing "Validate Stream Routes should raise exception if stream routes are empty"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {}))))

    (testing "Validate Stream Routes should raise exception if stream route does not have handler-fn"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:booking {}}))))

    (testing "Validate Stream Routes should raise exception if stream route does have nil value"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:booking nil}))))

    (testing "Validate Stream Routes should raise exception if stream route has nil handler-fn"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:booking {:handler-fn nil}}))))))

(deftest ziggurat-routes-serve-actor-routes-test
  (testing "The routes added by actor should be served along with ziggurat-routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() {:booking {:handler-fn (constantly :success)}} [["test-ping" (fn [_request] {:status 200
                                                                                                   :body   "pong"})]])
      (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
            status-actor status
            {:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #())
        (is (= 200 status-actor))
        (is (= 200 status)))))

  (testing "The routes not added by actor should return 404"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() [] [])
      (let [{:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)]
        (init/stop #())
        (is (= 404 status)))))

  (testing "The ziggurat routes should work fine when actor routes are not provided"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() [] [])
      (let [{:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #())
        (is (= 200 status))))))
