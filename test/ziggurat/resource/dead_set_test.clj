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
