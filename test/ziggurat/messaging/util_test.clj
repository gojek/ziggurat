(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :refer [prefixed-queue-name get-channel-names]]))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-entity and queue-name, it returns a snake case string with topic-entity as the prefix of queue-name"
    (let [topic-entity    :topic-name
          queue-name      "queue_name"
          expected-string "topic-name_queue_name"]
      (is (= (prefixed-queue-name topic-entity queue-name) expected-string)))))

(deftest get-channel-names-test
  (testing "Should remove both, :handler and :handler-fn keys when both of them are present in stream-routes"
    (let [stream-routes {:geofence-driver-ping {:handler #() :handler-fn #() :some-key "some-value"}}
          result (set (get-channel-names stream-routes :geofence-driver-ping))]
      (is (contains? result :some-key))
      (is (= 1 (count result))))))
