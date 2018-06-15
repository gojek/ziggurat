(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :refer [get-value-with-prefix-topic]]))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-entity and queue-name, it returns a string with topic-entity as the prefix of queue-name"
    (let [topic-entity    :topic
          queue-name      "queue_name"
          expected-string "topic_queue_name"]
      (is (= (get-value-with-prefix-topic topic-entity queue-name) expected-string)))))
