(ns ziggurat.server.routes-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.resource.dead-set :refer [retry-enabled? channel-retry-enabled?]]
            [ziggurat.fixtures :as fix]
            [ziggurat.server.test-utils :as tu]
            [ziggurat.server :refer [server]]
            [cheshire.core :as json]))

(use-fixtures :each fix/silence-logging)

(deftest router-dead-set-channel-disabled-test
  (let [stream-routes {:default {:handler-fn (fn [])
                                 :channel-1 (fn [])}}]
    (with-redefs [retry-enabled? (fn [] false)]
      (fix/with-start-server
        stream-routes
        (testing "should return 404 when /v1/dead_set/replay for channel is called and channel retry is disabled"
          (with-redefs [channel-retry-enabled? (constantly false)]
            (let [params {:count "10" :topic-entity "default" :channel "channel-1"}
                  {:keys [status body]} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" params)
                  expected-body {:error "Retry is not enabled"}]
              (is (= 404 status))
              (is (= expected-body (json/decode body true))))))

        (testing "should return 404 when /v1/dead_set for channel is called and channel retry is disabled"
          (with-redefs [channel-retry-enabled? (constantly false)]
            (let [params {:count "10" :topic-entity "default" :channel "channel-1"}
                  {:keys [status body]} (tu/get (-> (ziggurat-config) :http-server :port) "/v1/dead_set" true true {} params)
                  expected-body {:error "Retry is not enabled"}]
              (is (= 404 status))
              (is (= expected-body body)))))

        (testing "should return 404 when delete /v1/dead_set for channel is called and channel retry is disabled"
          (with-redefs [channel-retry-enabled? (constantly false)]
            (let [params {:count "10" :topic-entity "default" :channel "channel-1"}
                  {:keys [status body]} (tu/delete (-> (ziggurat-config) :http-server :port) "/v1/dead_set" true true {} params)
                  expected-body {:error "Retry is not enabled"}]
              (is (= 404 status))
              (is (= expected-body body)))))))))

(deftest router-dead-set-disabled-test
  (let [stream-routes {:default {:handler-fn (fn [])}}]
    (with-redefs [retry-enabled? (fn [] false)]
      (fix/with-start-server
        stream-routes
        (testing "should return 404 when /v1/dead_set is called and retry is disabled"
          (with-redefs [retry-enabled? (constantly false)]
            (let [{:keys [status body]} (tu/get (-> (ziggurat-config) :http-server :port) "/v1/dead_set" true true {} nil)
                  expected-body {:error "Retry is not enabled"}]
              (is (= 404 status))
              (is (= expected-body body)))))

        (testing "should return 404 when delete /v1/dead_set is called and retry is disabled"
          (with-redefs [retry-enabled? (constantly false)]
            (let [{:keys [status body]} (tu/delete (-> (ziggurat-config) :http-server :port) "/v1/dead_set" true true {} nil)
                  expected-body {:error "Retry is not enabled"}]
              (is (= 404 status))
              (is (= expected-body body)))))

        (testing "should return 404 when /v1/dead_set/replay is called and retry is disabled"
          (let [{:keys [status body]} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {})
                expected-body {:error "Retry is not enabled"}]
            (is (= 404 status))
            (is (= expected-body (json/decode body true)))))))))

(deftest router-dead-set-enabled-test
  (let [stream-routes {:default {:handler-fn (fn [])}}]
    (with-redefs [retry-enabled? (fn [] true)]
      (fix/with-start-server
        stream-routes
        (testing "Should return 200 ok when GET /ping is called"
          (let [{:keys [status _]} (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false)]
            (is (= 200 status))))

        (testing "should return 200 when /v1/dead_set/replay is called with valid count val"
          (with-redefs [ds/replay (fn [_ _ _] nil)]
            (let [count "10"
                  params {:count count :topic-entity "default"}
                  {:keys [status _]} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" params)]
              (is (= 200 status)))))

        (testing "should return 400 when /v1/dead_set/replay is called with invalid count val"
          (with-redefs [ds/replay (fn [_ _ _] nil)]
            (let [count "10"
                  expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                  {:keys [status body]} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
              (is (= 400 status))
              (is (= expected-body (json/decode body true))))))

        (testing "should return 400 when /v1/dead_set/replay is called with no topic entity"
          (with-redefs [ds/replay (fn [_ _ _] nil)]
            (let [count "10"
                  expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                  {:keys [status body]} (tu/post (-> (ziggurat-config) :http-server :port) "/v1/dead_set/replay" {:count count})]
              (is (= 400 status))
              (is (= expected-body (json/decode body true))))))

        (testing "should return 400 when get /v1/dead_set is called with invalid count val"
          (with-redefs [ds/view (fn [_ _ _] nil)]
            (let [count "avasdas"
                  topic-entity "default"
                  params {:count count :topic-name topic-entity}
                  {:keys [status _]} (tu/get (-> (ziggurat-config) :http-server :port)
                                             "/v1/dead_set"
                                             true
                                             true
                                             {}
                                             params)]
              (is (= 400 status)))))

        (testing "should return 400 when get /v1/dead_set is called with negative count val"
          (with-redefs [ds/view (fn [_ _ _] nil)]
            (let [count "-10"
                  topic-entity "default"
                  params {:count count :topic-name topic-entity}
                  {:keys [status _]} (tu/get (-> (ziggurat-config) :http-server :port)
                                             "/v1/dead_set"
                                             true
                                             true
                                             {}
                                             params)]
              (is (= 400 status)))))

        (testing "should return 400 when get /v1/dead_set is called without topic entity"
          (with-redefs [ds/view (fn [_ _ _] nil)]
            (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                  count "10"
                  params {:count count}
                  {:keys [status body]} (tu/get (-> (ziggurat-config) :http-server :port)
                                                "/v1/dead_set"
                                                true
                                                true
                                                {}
                                                params)]
              (is (= 400 status))
              (is (= expected-body body)))))

        (testing "should return 400 when get /v1/dead_set is called with invalid channel"
          (with-redefs [channel-retry-enabled? (constantly true)]
            (with-redefs [ds/view (fn [_ _ _] nil)]
              (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                    count "10"
                    params {:count count :topic-entity "default" :channel "invalid"}
                    {:keys [status body]} (tu/get (-> (ziggurat-config) :http-server :port)
                                                  "/v1/dead_set"
                                                  true
                                                  true
                                                  {}
                                                  params)]
                (is (= 400 status))
                (is (= expected-body body))))))

        (testing "should return 200 when get /v1/dead_set is called with valid count val"
          (with-redefs [ds/view (fn [_ _ _] {:foo "bar"})]
            (let [count "10"
                  params {:count count :topic-entity "default"}
                  {:keys [status body]} (tu/get (-> (ziggurat-config) :http-server :port)
                                                "/v1/dead_set"
                                                true
                                                true
                                                {}
                                                params)]
              (is (= 200 status))
              (is (= (:messages body) {:foo "bar"})))))

        (testing "should return 200 when delete /v1/dead_set is called with valid parameters"
          (with-redefs [ds/delete (fn [_ _ _] {:foo "bar"})]
            (let [count "10"
                  params {:count count :topic-entity "default"}
                  {:keys [status body]} (tu/delete (-> (ziggurat-config) :http-server :port)
                                                   "/v1/dead_set"
                                                   true
                                                   true
                                                   {}
                                                   params)]
              (is (= 200 status))
              (is (= (:message body) "Deleted messages successfully")))))

        (testing "should return 400 when delete /v1/dead_set is called with invalid count val"
          (with-redefs [ds/delete (fn [_ _ _] nil)]
            (let [count "avasdas"
                  topic-entity "default"
                  params {:count count :topic-name topic-entity}
                  {:keys [status _]} (tu/delete (-> (ziggurat-config) :http-server :port)
                                                "/v1/dead_set"
                                                true
                                                true
                                                {}
                                                params)]
              (is (= 400 status)))))

        (testing "should return 400 when delete /v1/dead_set is called with negative count val"
          (with-redefs [ds/delete (fn [_ _ _] nil)]
            (let [count "-10"
                  topic-entity "default"
                  params {:count count :topic-name topic-entity}
                  {:keys [status _]} (tu/delete (-> (ziggurat-config) :http-server :port)
                                                "/v1/dead_set"
                                                true
                                                true
                                                {}
                                                params)]
              (is (= 400 status)))))

        (testing "should return 400 when delete /v1/dead_set is called without topic entity"
          (with-redefs [ds/delete (fn [_ _ _] nil)]
            (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                  count "10"
                  params {:count count}
                  {:keys [status body]} (tu/delete (-> (ziggurat-config) :http-server :port)
                                                   "/v1/dead_set"
                                                   true
                                                   true
                                                   {}
                                                   params)]
              (is (= 400 status))
              (is (= expected-body body)))))

        (testing "should return 400 when delete /v1/dead_set is called with invalid channel"
          (with-redefs [channel-retry-enabled? (constantly true)]
            (with-redefs [ds/view (fn [_ _ _] nil)]
              (let [expected-body {:error "Count should be the positive integer and topic entity/ channel should be present"}
                    count "10"
                    params {:count count :topic-entity "default" :channel "invalid"}
                    {:keys [status body]} (tu/delete (-> (ziggurat-config) :http-server :port)
                                                     "/v1/dead_set"
                                                     true
                                                     true
                                                     {}
                                                     params)]
                (is (= 400 status))
                (is (= expected-body body))))))))))
