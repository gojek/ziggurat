(ns ziggurat.server.routes-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.fixtures :as fix]
            [ziggurat.server.test-utils :as tu]
            [ziggurat.server :refer [server]]
            [cheshire.core :as json]))

(use-fixtures :each fix/silence-logging)

(deftest router-test
  (let [stream-routes {:booking {:handler-fn (fn [])}}]
    (fix/with-start-server
      stream-routes
      (testing "Should return 200 ok when GET /ping is called"
        (let [{:keys [status _] :as response} (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false)]
          (is (= 200 status))))

      (testing "should return 200 when /v1/dead_set/replay is called with valid count val"
        (with-redefs [ds/replay (fn [_ _ _] nil)]
          (let [count  "10"
                params {:count count :topic-entity "booking"}
                {:keys [status _] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" params)]
            (is (= 200 status)))))

      (testing "should return 400 when /v1/dead_set/replay is called with invalid count val"
        (with-redefs [ds/replay (fn [_ _ _] nil)]
          (let [count         "10"
                expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
            (is (= 400 status))
            (is (= expected-body (json/decode body true))))))

      (testing "should return 400 when /v1/dead_set/replay is called with no topic entity"
        (with-redefs [ds/replay (fn [_ _ _] nil)]
          (let [count         "10"
                expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                {:keys [status body] :as response} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
            (is (= 400 status))
            (is (= expected-body (json/decode body true))))))

      (testing "should return 400 when get /v1/dead_set is called with invalid count val"
        (with-redefs [ds/view (fn [_ _ _] nil)]
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
        (with-redefs [ds/view (fn [_ _ _] nil)]
          (let [count        "-10"
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
        (with-redefs [ds/view (fn [_ _ _] nil)]
          (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
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

      (testing "should return 400 when get /v1/dead_set is called with invalid channel"
        (with-redefs [ds/view (fn [_ _ _] nil)]
          (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                count         "10"
                params        {:count count :topic-entity "booking" :channel "invalid"}
                {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                           "/v1/dead_set"
                                                           true
                                                           true
                                                           {}
                                                           params)]
            (is (= 400 status))
            (is (= expected-body body)))))

      (testing "should return 200 when get /v1/dead_set is called with valid count val"
        (with-redefs [ds/view (fn [_ _ _] {:foo "bar"})]
          (let [count  "10"
                params {:count count :topic-entity "booking"}
                {:keys [status body] :as response} (tu/get (-> (ziggurat-config) :http-server :port)
                                                           "/v1/dead_set"
                                                           true
                                                           true
                                                           {}
                                                           params)]
            (is (= 200 status))
            (is (= (:messages body) {:foo "bar"}))))))))
