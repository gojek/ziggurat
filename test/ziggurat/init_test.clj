(ns ziggurat.init-test
  (:require [clojure.test :refer :all]
            [mount.core :refer [defstate] :as mount]
            [ziggurat.config :as config]
            [ziggurat.init :as init]
            [ziggurat.messaging.connection :as rmqc]
            [ziggurat.messaging.consumer :as messaging-consumer]
            [ziggurat.messaging.producer :as messaging-producer]
            [ziggurat.streams :as streams :refer [stream]]
            [ziggurat.server.test-utils :as tu]
            [ziggurat.tracer :as tracer]
            [ziggurat.fixtures :refer [with-config]]
            [cambium.logback.json.flat-layout :as flat]
            [cambium.codec :as codec]
            [cambium.core :as clog]
            [clojure.tools.logging :as log])

  (:import (io.opentracing.mock MockTracer)))

(defn exp [x n]
  (if (zero? n) 1
      (* x (exp x (dec n)))))

(deftest start-calls-actor-start-fn-test
  (testing "The actor start fn starts before the ziggurat state and can read config"
    (let [result                              (atom 1)]
      (with-redefs [streams/start-streams (fn [_ _] (reset! result (* @result 2)))
                    streams/stop-streams  (constantly nil)
                    ;; will be called valid modes number of times
                    rmqc/start-connection (fn [] (reset! result (* @result 2)))
                    rmqc/stop-connection  (constantly nil)
                    tracer/create-tracer  (fn [] (MockTracer.))]
        (with-config
          (do (init/start #(reset! result (+ @result 3)) {} {} [] nil)
              (init/stop #() nil)
              (is (= 16 @result))))))))

(deftest stop-calls-actor-stop-fn-test
  (testing "stops streaming before calling the actor stop function"
    (let [external-state (atom true)
          stop-fn        #(reset! external-state false)]
      (with-redefs [streams/start-streams (constantly nil)
                    streams/stop-streams  (fn [_] (is (true? @external-state)))
                    tracer/create-tracer  (fn [] (MockTracer.))]
        (with-config
          (do (init/start #() {} {} [] nil)
              (init/stop stop-fn nil)
              (is (false? @external-state))))))))

(deftest stop-calls-idempotentcy-test
  (testing "The stop function should be idempotent"
    (let [result                              (atom 1)
          expected-count 5]
      (with-redefs [streams/start-streams (constantly nil)
                    streams/stop-streams  (constantly nil)
                    rmqc/stop-connection  (fn [_] (reset! result (* @result 2)))
                    tracer/create-tracer  (fn [] (MockTracer.))]
        (with-config
          (do (init/start #() {} {} [] nil)
              (init/stop #(reset! result (+ @result 3)) nil)
              (is (= expected-count @result))))))))

(deftest start-calls-make-queues-with-both-streams-and-batch-routes-test
  (testing "Start calls make queues with both streams and batch routes"
    (let [make-queues-called     (atom 0)
          expected-stream-routes {:default {:handler-fn #()}}
          expected-batch-routes  {:consumer-1 {:handler-fn #()}}]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-producer/make-queues       (fn [all-routes]
                                                           (swap! make-queues-called + 1)
                                                           (is (= all-routes (merge expected-stream-routes expected-batch-routes))))
                    messaging-consumer/start-subscribers (constantly nil)
                    config/config-file                   "config.test.edn"
                    tracer/create-tracer                 (fn [] (MockTracer.))]
        (with-config
          (do (init/start #() expected-stream-routes expected-batch-routes [] nil)
              (init/stop #() nil)
              (is (= 2 @make-queues-called))))))))

(deftest start-calls-start-subscribers-test
  (testing "Start calls start subscribers"
    (let [start-subscriber-called (atom 0)
          expected-stream-routes  {:default {:handler-fn #()}}
          expected-batch-routes   {:consumer-1 {:handler-fn #()}}]
      (with-redefs [streams/start-streams                (constantly nil)
                    streams/stop-streams                 (constantly nil)
                    messaging-consumer/start-subscribers (fn [stream-routes batch-routes]
                                                           (swap! start-subscriber-called + 1)
                                                           (is (= stream-routes expected-stream-routes))
                                                           (is (= batch-routes expected-batch-routes)))
                    messaging-producer/make-queues       (constantly nil)
                    config/config-file                   "config.test.edn"
                    tracer/create-tracer                 (fn [] (MockTracer.))]
        (with-config
          (do
            (init/start #() expected-stream-routes expected-batch-routes [] nil)
            (init/stop #() nil)
            (is (= 1 @start-subscriber-called))))))))

(deftest main-test
  (testing "Main function should call start (arity: 3)"
    (let [start-was-called       (atom false)
          expected-stream-routes {:default {:handler-fn #(constantly nil)}}]
      (with-redefs [init/add-shutdown-hook              (fn [_ _] (constantly nil))
                    init/initialize-config              (constantly nil)
                    init/validate-routes-against-config (constantly nil)
                    init/start                          (fn [_ stream-router _ _ _]
                                                          (swap! start-was-called not)
                                                          (is (= expected-stream-routes stream-router)))]
        (init/main #() #() expected-stream-routes)
        (is @start-was-called))))
  (testing "Flat Json Layout decoder is set if log format is json"
    (let [start-was-called       (atom false)
          decoder-was-set        (atom false)
          expected-stream-routes {:default {:handler-fn #(constantly nil)}}
          config                 config/default-config]
      (with-redefs [init/add-shutdown-hook              (fn [_ _] (constantly nil))
                    config/config-file                  "config.test.edn"
                    config/ziggurat-config              (fn [] (assoc config :log-format "json"))
                    init/validate-routes-against-config (constantly nil)
                    init/start                          (fn [_ stream-router _ _ _]
                                                          (swap! start-was-called not)
                                                          (is (= expected-stream-routes stream-router)))
                    flat/set-decoder!                   (fn [decoder] (is (= decoder codec/destringify-val))
                                                          (reset! decoder-was-set true))]
        (init/main #() #() expected-stream-routes)
        (is @start-was-called)
        (is @decoder-was-set)))))

(def mock-modes {:api-server     {:start-fn (constantly nil) :stop-fn (constantly nil)}
                 :stream-worker  {:start-fn (constantly nil) :stop-fn (constantly nil)}
                 :worker         {:start-fn (constantly nil) :stop-fn (constantly nil)}
                 :batch-worker   {:start-fn (constantly nil) :stop-fn (constantly nil)}
                 :management-api {:start-fn (constantly nil) :stop-fn (constantly nil)}})

(deftest batch-routes-test
  (testing "Main function should start batch consumption if batch-routes are provided and the modes vector is empty (arity: 1)"
    (let [start-batch-consumers-was-called (atom false)
          expected-stream-routes           {:default {:handler-fn #(constantly nil)}}
          batch-routes                     {:consumer-1 {:handler-fn #(constantly nil)}}]
      (with-redefs [init/add-shutdown-hook              (constantly nil)
                    init/start-common-states            (constantly nil)
                    init/initialize-config              (constantly nil)
                    init/validate-routes-against-config (constantly nil)
                    init/valid-modes-fns                (assoc-in mock-modes [:batch-worker :start-fn] (fn [_] (reset! start-batch-consumers-was-called true)))]
        (init/main {:start-fn #() :stop-fn #() :stream-routes expected-stream-routes :batch-routes batch-routes :actor-routes []})
        (is @start-batch-consumers-was-called)))))

(deftest stream-routes-test
  (testing "Main function should call the start the streams when the (arity: 4)"
    (let [start-streams-called   (atom false)
          expected-stream-routes {:default {:handler-fn #(constantly nil)}}]
      (with-redefs [init/add-shutdown-hook              (constantly nil)
                    init/start-common-states            (constantly nil)
                    init/initialize-config              (constantly nil)
                    init/validate-routes-against-config (constantly nil)
                    init/valid-modes-fns                (assoc-in mock-modes [:stream-worker :start-fn] (fn [_] (reset! start-streams-called true)))]
        (init/main #() #() expected-stream-routes)
        (is @start-streams-called)))))

(deftest validate-events-routes-test
  (with-redefs [config/config-file "config.test.edn"]
    (with-config
      (do
        (let [exception-message "Invalid stream routes"]
          (testing "Validate Stream Routes should raise exception if stream routes is nil and stream worker is one of the modes"
            (is (thrown? RuntimeException exception-message (init/validate-routes nil {:consumer-1 {:handler-fn #()}} [:stream-worker]))))

          (testing "Validate Stream Routes should raise exception if stream routes are empty and stream worker is one of the modes"
            (is (thrown? RuntimeException exception-message (init/validate-routes {} {:consumer-1 {:handler-fn #()}} [:stream-worker]))))

          (testing "Validate Stream Routes should raise exception if stream route does not have handler-fn and stream worker is one of the modes"
            (is (thrown? RuntimeException exception-message (init/validate-routes {:default {}} {:consumer-1 {:handler-fn #()}} [:stream-worker]))))

          (testing "Validate Stream Routes should raise exception if stream route does have nil value and stream worker is one of the modes"
            (is (thrown? RuntimeException exception-message (init/validate-routes {:default nil} {:consumer-1 {:handler-fn #()}} [:stream-worker]))))

          (testing "Validate Stream Routes should raise exception if stream route has nil handler-fn and stream worker is one of the modes"
            (is (thrown? RuntimeException exception-message (init/validate-routes {:default {:handler-fn nil}} {:consumer-1 {:handler-fn #()}} [:stream-worker]))))

          (testing "Does not throw an exception if validation is successful"
            (let [stream-route {:default {:handler-fn (fn [])
                                          :channel-1  (fn [])}}
                  batch-route  {:consumer-1 {:handler-fn #()}}]
              (init/validate-routes stream-route batch-route [:stream-worker :batch-worker])))
          (testing "stream-worker present in modes and stream routes not present should throw an exception"
            (is (thrown? Exception (init/validate-routes nil nil [:api-server :stream-worker]))))
          (testing "batch-worker present in modes and batch routes not present should throw an exception"
            (is (thrown? Exception (init/validate-routes nil nil [:api-server :batch-worker]))))
          (testing "batch-worker and stream-worker present in modes and batch routes and stream routes present should not throw an exception"
            (init/validate-routes {:default {:handler-fn (fn [])}} {:consumer-1 {:handler-fn (fn [])}} [:api-server :stream-worker :batch-worker]))
          (testing "actor routes present in modes and arguments should return nil"
            (is (nil? (init/validate-routes nil nil [:api-server])))))))))

(deftest ziggurat-routes-serve-actor-routes-test
  (testing "The routes added by actor should be served along with ziggurat-routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  tracer/create-tracer  (fn [] (MockTracer.))]
      (with-config
        (do (init/start #() {} {} [["test-ping" (fn [_request] {:status 200
                                                                :body   "pong"})]] nil)
            (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
                  status-actor status
                  {:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
              (init/stop #() nil)
              (is (= 200 status-actor))
              (is (= 200 status)))))))

  (testing "Deadset management and server api modes should run both actor and deadset management routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  tracer/create-tracer  (fn [] (MockTracer.))]
      (with-config
        (do (init/start #() {} {} [["test-ping" (fn [_request] {:status 200
                                                                :body   "pong"})]] [:management-api :api-server])
            (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
                  status-actor status
                  {:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
              (init/stop #() nil)
              (is (= 200 status-actor))
              (is (= 200 status)))))))

  (testing "The routes not added by actor should return 404"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  tracer/create-tracer  (fn [] (MockTracer.))]
      (with-config
        (do (init/start #() {} {} [] nil)
            (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)]
              (init/stop #() nil)
              (is (= 404 status)))))))

  (testing "The ziggurat routes should work fine when actor routes are not provided"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams  (constantly nil)
                  tracer/create-tracer  (fn [] (MockTracer.))]
      (with-config
        (do (init/start #() {} {} [] nil)
            (let [{:keys [status]} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
              (init/stop #() nil)
              (is (= 200 status))))))))

(deftest validate-modes-test
  (testing "Validate modes should raise exception if modes have any invalid element"
    (let [modes [:invalid-modes :api-server :second-invalid]]
      (is (thrown? clojure.lang.ExceptionInfo (init/validate-modes modes nil nil nil)))))
  (testing "Validate modes should return the list of modes provided by the user, if all modes are valid"
    (let [modes [:stream-worker :api-server :batch-worker]]
      (is (= modes (init/validate-modes modes nil nil nil)))))
  (testing "Validate modes should return the modes derived from routes if no modes are provided by the user"
    (is (= [:management-api :worker :stream-worker :batch-worker :api-server]
           (init/validate-modes nil {:stream-1 {:handler-fn #()}} {:consumer-1 {:handler-fn #()}} []))))
  (testing "Validate modes should return [:management-api :worker :stream-worker] if only stream-routes are provided by the user"
    (is (= [:management-api :worker :stream-worker]
           (init/validate-modes nil {:stream-1 {:handler-fn #()}} nil nil))))
  (testing "Validate modes should return [:management-api :worker :batch-worker] if only batch-routes are provided by the user"
    (is (= [:management-api :worker :batch-worker]
           (init/validate-modes nil nil {:consumer-1 {:handler-fn #()}} nil))))
  (testing "Validate modes should return [:management-api :worker :stream-worker :batch-worker] if both stream-routes and batch-routes are provided by the user"
    (is (= [:management-api :worker :stream-worker :batch-worker]
           (init/validate-modes nil {:stream-1 {:handler-fn #()}} {:consumer-1 {:handler-fn #()}} nil))))
  (testing "Validate modes should throw an IllegalArgumentException if modes are not provided explicitly and neither :stream-routes and :batch-routes keys are present in init args"
    (is (thrown? IllegalArgumentException (init/validate-modes nil nil nil [])))))

(deftest kafka-producers-should-start
  (let [args                 {:actor-routes  []
                              :stream-routes {}
                              :batch-routes  {}}
        producer-has-started (atom false)]
    (with-redefs [init/start-kafka-producers (fn [] (reset! producer-has-started true))
                  init/start-kafka-streams   (constantly nil)]
      (testing "Starting the streams should start kafka-producers as well"
        (init/start-stream args)
        (is (= true @producer-has-started)))
      (testing "Starting the workers should start kafka-producers as well"
        (reset! producer-has-started false)
        (init/start-workers args)
        (is (= true @producer-has-started))))))

(deftest kafka-producers-should-stop
  (let [producer-has-stopped (atom false)]
    (with-redefs [init/stop-kafka-producers (fn [] (reset! producer-has-stopped true))
                  init/stop-kafka-streams   (constantly nil)]
      (testing "Stopping the streams should stop kafka-producers as well"
        (init/stop-stream)
        (is (= true @producer-has-stopped)))
      (testing "Stopping the workers should stop kafka-producers as well"
        (reset! producer-has-stopped false)
        (init/stop-workers)
        (is (= true @producer-has-stopped)))
      (mount/stop))))

(deftest validate-routes-against-config-test
  (with-config
    (let [stream-routes {:test-router {:handler-fn #(constantly nil)}}
          batch-routes  {:test-consumer {:handler-fn #(constantly nil)}}
          modes         [:stream-worker :batch-worker]]

      (with-redefs [config/ziggurat-config (fn [] (-> config/config
                                                      :ziggurat
                                                      (assoc-in [:stream-router] {:test-router {:handler-fn #(constantly nil)
                                                                                                :channels   {:channel-1 {}}}})
                                                      (assoc-in [:batch-routes] {:test-consumer {:handler-fn #(constantly nil)}})))]
        (testing "when routes which are present in the config are passed, there isn't any exception"
          (init/validate-routes stream-routes batch-routes modes))
        (testing "when invalid stream-routes is passed, there is an exception"
          (let [stream-routes {:test-router-invalid {:handler-fn #(constantly nil)}}]
            (is (thrown? IllegalArgumentException (init/validate-routes stream-routes batch-routes modes)))))
        (testing "when invalid stream-route is passed along with a valid one, there is an exception"
          (let [stream-routes {:test-router         {:handler-fn #(constantly nil)}
                               :test-router-invalid {:handler-fn #(constantly nil)}}]
            (is (thrown? IllegalArgumentException (init/validate-routes stream-routes batch-routes modes)))))
        (testing "when invalid batch-routes is passed, there is an exception"
          (let [batch-routes {:test-consumer-invalid {:handler-fn #(constantly nil)}}]
            (is (thrown? IllegalArgumentException (init/validate-routes stream-routes batch-routes modes)))))
        (testing "when invalid batch-routes is passed along with a valid one, there is an exception"
          (let [batch-routes {:test-consumer         {:handler-fn #(constantly nil)}
                              :test-consumer-invalid {:handler-fn #(constantly nil)}}]
            (is (thrown? IllegalArgumentException (init/validate-routes stream-routes batch-routes modes)))))
        (testing "when invalid channels in stream-routes is passed, there is an exception"
          (let [stream-routes {:test-router {:handler-fn #(constantly nil)
                                             :channel-1  #()
                                             :channel-2  #()}}]
            (is (thrown? IllegalArgumentException (init/validate-routes stream-routes batch-routes modes)))))))))

