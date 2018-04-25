(ns ziggurat.server.routes-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.fixtures :as fix]
            [ziggurat.server.test-utils :as tu]))

(use-fixtures :once fix/start-server)

(deftest router-test
  (testing "Should return 200 ok when GET /ping is called"
    (let [{:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false)]
      (is (= 200 status))))

  (testing "should return 200 when /v1/dead_set/replay is called with valid count val"
    (with-redefs [ds/replay (fn [_] nil)]
      (let [count 10
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 200 status)))))

  (testing "should return 400 when /v1/dead_set/replay is called with invalid count val"
    (with-redefs [ds/replay (fn [_] nil)]
      (let [count "10"
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 400 status)))))

  (testing "should return 400 when get /v1/dead_set/replay is called with invalid count val"
    (with-redefs [ds/view (fn [_] nil)]
      (let [count "avasdas"
            {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                       "/v1/dead_set/replay"
                                                       true
                                                       true
                                                       {}
                                                       {:count count})]
        (is (= 400 status)))))

  (testing "should return 400 when get /v1/dead_set/replay is called with invalid count val"
    (with-redefs [ds/view (fn [_] {:foo "bar"})]
      (let [count 10
            {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                       "/v1/dead_set/replay"
                                                       true
                                                       true
                                                       {}
                                                       {:count count})]
        (is (= 200 status))
        (is (= (:messages body) {:foo "bar"}))))))
