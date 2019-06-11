(ns ziggurat.init-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.init :as init]
            [ziggurat.messaging.connection :as rmqc]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.streams :as streams]
            [ziggurat.server.test-utils :as tu]))

(deftest start-calls-actor-start-fn-test
  (testing "The actor start fn starts before the ziggurat state and can read config"
    (let [result (atom 1)]
      (with-redefs [streams/start-streams (fn [_ _] (reset! result (* @result 2)))
                    streams/stop-streams  (constantly nil)
                    rmqc/start-connection (fn [] (reset! result (* @result 2)))
                    rmqc/stop-connection  (constantly nil)
                    config/config-file    "config.test.edn"]
        (init/start #(reset! result (+ @result 3)) {} [] nil)
        (init/stop #() nil)
        (is (= 16 @result))))))

(deftest stop-calls-actor-stop-fn-test
  (testing "The actor stop fn stops before the ziggurat state"
    (let [result (atom 1)]
      (with-redefs [streams/start-streams (constantly nil)
                    streams/stop-streams  (fn [_] (reset! result (* @result 2)))
                    config/config-file    "config.test.edn"]
        (init/start #() {} [] nil)
        (init/stop #(reset! result (+ @result 3)) nil)
        (is (= 8 @result))))))

(deftest stop-calls-idempotentcy-test
  (testing "The stop function should be idempotent"
    (let [result (atom 1)]
      (with-redefs [streams/start-streams (constantly nil)
                    streams/stop-streams  (constantly nil)
                    rmqc/stop-connection (fn [_] (reset! result (* @result 2)))
                    config/config-file    "config.test.edn"]
        (init/start #() {} [] nil)
        (init/stop #(reset! result (+ @result 3)) nil)
        (is (= 8 @result))))))

(deftest start-calls-make-queues-test
  (testing "Start calls make queues"
    (let [make-queues-called     (atom 0)
          expected-stream-routes {:default {:handler-fn #()}}]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-producer/make-queues       (fn [stream-routes]
                                                           (swap! make-queues-called + 1)
                                                           (is (= stream-routes expected-stream-routes)))
                    messaging-consumer/start-subscribers (constantly nil)
                    config/config-file                   "config.test.edn"]
        (init/start #() expected-stream-routes [] nil)
        (init/stop #() nil)
        (is (= 2 @make-queues-called))))))

(deftest start-calls-start-subscribers-test
  (testing "Start calls start subscribers"
    (let [start-subscriber-called (atom 0)
          expected-stream-routes  {:default {:handler-fn #()}}]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-consumer/start-subscribers (fn [stream-routes]
                                                           (swap! start-subscriber-called + 1)
                                                           (is (= stream-routes expected-stream-routes)))
                    messaging-producer/make-queues       (constantly nil)
                    config/config-file                   "config.test.edn"]
        (init/start #() expected-stream-routes [] nil)
        (init/stop #() nil)
        (is (= 1 @start-subscriber-called))))))

(deftest main-test
  (testing "Main function should call start"
    (let [start-was-called       (atom false)
          expected-stream-routes {:default {:handler-fn #(constantly nil)}}]
      (with-redefs [init/add-shutdown-hook (fn [_ _] (constantly nil))
                    init/start             (fn [_ stream-router _ _]
                                             (swap! start-was-called not)
                                             (is (= expected-stream-routes stream-router)))]
        (init/main #() #() expected-stream-routes)
        (is @start-was-called)))))

(deftest validate-stream-routes-test
  (let [exception-message "Invalid stream routes"]
    (testing "Validate Stream Routes should raise exception if stream routes is nil and stream worker is one of the modes"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes nil [:stream-worker]))))

    (testing "Validate Stream Routes should raise exception if stream routes are empty and stream worker is one of the modes"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {} [:stream-worker]))))

    (testing "Validate Stream Routes should raise exception if stream route does not have handler-fn and stream worker is one of the modes"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:default {}} [:stream-worker]))))

    (testing "Validate Stream Routes should raise exception if stream route does have nil value and stream worker is one of the modes"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:default nil} [:stream-worker]))))

    (testing "Validate Stream Routes should raise exception if stream route has nil handler-fn and stream worker is one of the modes"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:default {:handler-fn nil}} [:stream-worker]))))

    (testing "Validate Stream Routes should raise exception if stream route has nil handler-fn and there is no mode passed"
      (is (thrown? RuntimeException exception-message (init/validate-stream-routes {:default {:handler-fn nil}} nil)))))

  (testing "Validate Stream Routes should return nil if stream route is empty or nil and stream worker is not one of the modes"
    (is (nil? (init/validate-stream-routes nil [:api-server])))
    (is (nil? (init/validate-stream-routes {} [:api-server]))))

  (testing "Validate Stream Routes should raise exception if stream route has nil handler-fn"
    (let [stream-route {:default {:handler-fn (fn [])
                                  :channel-1  (fn [])
                                  :channel-2  (fn [])}}]
      (is (= stream-route (init/validate-stream-routes stream-route [:stream-worker]))))))

(deftest ziggurat-routes-serve-actor-routes-test
  (testing "The routes added by actor should be served along with ziggurat-routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() {} [["test-ping" (fn [_request] {:status 200
                                                       :body   "pong"})]] nil)
      (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
            status-actor status
            {:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #() nil)
        (is (= 200 status-actor))
        (is (= 200 status)))))

  (testing "Deadset management and server api modes should run both actor and deadset management routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() {} [["test-ping" (fn [_request] {:status 200
                                                       :body   "pong"})]] [:management-api :api-server])
      (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
            status-actor status
            {:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #() nil)
        (is (= 200 status-actor))
        (is (= 200 status)))))

  (testing "The routes not added by actor should return 404"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() {} [] nil)
      (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)]
        (init/stop #() nil)
        (is (= 404 status)))))

  (testing "The ziggurat routes should work fine when actor routes are not provided"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  config/config-file    "config.test.edn"]
      (init/start #() {} [] nil)
      (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #() nil)
        (is (= 200 status))))))

(deftest validate-modes-test
  (testing "Validate modes should raise exception if modes have any invalid element"
    (let [modes [:invalid-modes :api-server :second-invalid]]
      (is (thrown? clojure.lang.ExceptionInfo (init/validate-modes modes))))))

