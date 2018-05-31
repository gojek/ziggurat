(ns ziggurat.init-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.init :as init]
            [ziggurat.streams :as streams]
            [ziggurat.server.test-utils :as tu]))

(deftest start-calls-actor-start-fn
  (testing "The actor start fn starts after the lambda internal state and can read config"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams (constantly nil)
                  config/config-file "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #(deliver retry-count (-> (config/ziggurat-config) :retry :count)) #() [])
        (init/stop #())
        (is (= 5 (deref retry-count 10000 ::failure)))))))

(deftest stop-calls-actor-stop-fn
  (testing "The actor stop fn is called before stopping the lambda internal state"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams (constantly nil)
                  config/config-file "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #() #() [])
        (init/stop #(deliver retry-count (-> (config/ziggurat-config) :retry :count)))
        (is (= 5 (deref retry-count 10000 ::failure)))))))

(deftest ziggurat-routes-serve-actor-routes
  (testing "The routes added by actor should be served along with ziggurat-routes"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams (constantly nil)
                  config/config-file "config.test.edn"]
      (init/start #() #() [["test-ping" (fn [_request] {:status 200
                                                        :body   "pong"})]])
      (let [{:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)
            status-actor status
            {:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #())
        (is (= 200 status-actor))
        (is (= 200 status)))))

  (testing "The routes not added by actor should return 404"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams (constantly nil)
                  config/config-file "config.test.edn"]
      (init/start #() #() [])
      (let [{:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/test-ping" true false)]
        (init/stop #())
        (is (= 404 status)))))

  (testing "The ziggurat routes should work fine when actor routes are not provided"
    (with-redefs [streams/start-streams (constantly nil)
                  streams/stop-streams (constantly nil)
                  config/config-file "config.test.edn"]
      (init/start #() #() [])
      (let [{:keys [status body] :as response} (tu/get (-> (config/ziggurat-config) :http-server :port) "/ping" true false)]
        (init/stop #())
        (is (= 200 status))))))
