(ns ziggurat.server.routes-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.fixtures :as fix]
            [ziggurat.server.test-utils :as tu]
            [cheshire.core :as json]))

(use-fixtures :once fix/start-server fix/init-rabbit-mq)

(deftest router-test
  (testing "Should return 200 ok when GET /ping is called"
    (let [{:keys [status _] :as response} (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false)]
      (is (= 200 status))))

  (testing "should return 200 when /v1/dead_set/replay is called with valid count val"
    (with-redefs [ds/replay (fn [_ _] nil)]
      (let [count  10
            params {:count count :topic-entity "booking"}
            {:keys [status _] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" params)]
        (is (= 200 status)))))

  (testing "should return 400 when /v1/dead_set/replay is called with invalid count val"
    (with-redefs [ds/replay (fn [_ _] nil)]
      (let [count         "10"
            expected-body {:error "Count should be the positive integer and topic entity should be present"}
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 400 status))
        (is (= expected-body (json/decode body true))))))

  (testing "should return 400 when /v1/dead_set/replay is called with no topic entity"
    (with-redefs [ds/replay (fn [_ _] nil)]
      (let [count         10
            expected-body {:error "Count should be the positive integer and topic entity should be present"}
            {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
        (is (= 400 status))
        (is (= expected-body (json/decode body true))))))

  (testing "should return 400 when get /v1/dead_set is called with invalid count val"
    (with-redefs [ds/view (fn [_ _] nil)]
      (let [count        "avasdas"
            topic-entity "booking"
            params       {:count count :topic-name topic-entity}
            {:keys [status _] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                    "/v1/dead_set"
                                                    true
                                                    true
                                                    {}
                                                    params)]
        (is (= 400 status)))))

  (testing "should return 400 when get /v1/dead_set is called with negative count val"
    (with-redefs [ds/view (fn [_ _] nil)]
      (let [count        -10
            topic-entity "booking"
            params       {:count count :topic-name topic-entity}
            {:keys [status _] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                    "/v1/dead_set"
                                                    true
                                                    true
                                                    {}
                                                    params)]
        (is (= 400 status)))))

  (testing "should return 400 when get /v1/dead_set is called without topic entity"
    (with-redefs [ds/view (fn [_ _] nil)]
      (let [expected-body {:error "Count should be the positive integer and topic entity should be present"}
            count         "10"
            params        {:count count}
            {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                       "/v1/dead_set"
                                                       true
                                                       true
                                                       {}
                                                       params)]
        (is (= 400 status))
        (is (= expected-body body)))))

  (testing "should return 200 when get /v1/dead_set is called with valid count val"
    (with-redefs [ds/view (fn [_ _] {:foo "bar"})]
      (let [count      10
            params     {:count count :topic-entity "booking"}
            {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                       "/v1/dead_set"
                                                       true
                                                       true
                                                       {}
                                                       params)]
        (is (= 200 status))
        (is (= (:messages body) {:foo "bar"}))))))
