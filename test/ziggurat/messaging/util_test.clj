(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :refer [get-name-with-prefix-topic]]))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-name and name, it returns a string with topic-name as the prefix of that name"
    (let [topic-name "topic"
          name "queue_name"
          expected-string "topic_queue_name"]
      (is (= (get-name-with-prefix-topic topic-name name) expected-string))))
  (testing "it returns back the name if topic-name is nil"
    (let [topic-name nil
          name "queue_name"]
      (is (= (get-name-with-prefix-topic topic-name name) name)))))