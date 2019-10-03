(ns ziggurat.resource.dead-set-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.resource.dead-set :refer [retry-enabled? channel-retry-enabled?]]
            [ziggurat.config :refer [get-in-config channel-retry-config]]))

(deftest retry-enabled?-test
  (testing "returns value of retry enabled"
    (with-redefs [get-in-config (constantly [true :bool])]
      (let [expected-result [true :bool]]
        (is (= (retry-enabled?) expected-result))))))

(deftest channel-retry-enabled?-test
  (testing "returns value of retry enabled"
    (with-redefs [channel-retry-config (constantly {:count   [5 :int]
                                                    :enabled [true :bool]})]
      (let [expected-result [true :bool]
            topic-entity "booking"
            channel "channel-1"]
        (is (= (channel-retry-enabled? topic-entity channel) expected-result))))))

(deftest validate-params-test
  (testing "validate-param should return true if there's some value against a channel keyword in stream-routes"
    (let [validate-param-fun #'ziggurat.resource.dead-set/validate-params
          count 1
          result (validate-param-fun count :random-topic {:random-topic {:random-channel #()}} "random-channel")]
      (is (= count result))))
  (testing "validate-param should return true if there's some value against :handler-fn and no channel in stream-routes"
    (let [validate-param-fun #'ziggurat.resource.dead-set/validate-params
          count 1
          result (validate-param-fun count :random-topic {:random-topic {:handler-fn #()}} nil)]
      (is (= count result))))
  (testing "validate-param should return true if there's some value against :handler and no channel in stream-routes"
    (let [validate-param-fun #'ziggurat.resource.dead-set/validate-params
          count 1
          result (validate-param-fun count :random-topic {:random-topic {:handler #()}}  nil)]
      (is (= count result))))
  (testing "validate-param should return false if none of [channel, :handler-fn, :handler] has a value in stream-routes"
    (let [validate-param-fun #'ziggurat.resource.dead-set/validate-params
          count 1
          result (validate-param-fun count :random-topic {:random-topic {:key #()}}  "non-existent-channel")]
      (is (false? result)))))
