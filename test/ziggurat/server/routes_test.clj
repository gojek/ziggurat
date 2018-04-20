(ns ziggurat.server.routes-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.replay :as r]
            [ziggurat.fixtures :as fix]
            [ziggurat.server.test-utils :as tu]))

(use-fixtures :once fix/start-server)

(deftest router-test
  (testing "Should return 200 ok when GET /ping is called"
    (let [{:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false)]
      (is (= 200 status))))

  (testing "Should return 404 ok when put /ping is called"
    (let [{:keys [status body] :as response} (tu/put (-> (ziggurat-config) :http-server :port) "/ping" {})]
      (is (= 404 status))))

  (testing "should return 200 when /v1/dead_set/replay is called with valid count val"
    (with-redefs [r/replay (fn [_] nil)]
      (let [count 10
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 200 status)))))

  (testing "should return 400 when /v1/dead_set/replay is called with invalid count val"
    (with-redefs [r/replay (fn [_] nil)]
      (let [count "10"
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 400 status))))))
