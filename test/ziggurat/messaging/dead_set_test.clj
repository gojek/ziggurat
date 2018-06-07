(ns ziggurat.messaging.dead-set-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.fixtures :as fix]
            [ziggurat.messaging.dead-set :as ds]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))
;
;(use-fixtures :once fix/init-rabbit-mq)
;
;(deftest replay-test
;  (testing "puts message from dead set to instant queue"
;    (fix/with-clear-data
;      (let [count-of-messages 10
;            message           {:foo "bar"}
;            topic-name        "booking"
;            pushed-message    (doseq [counter (range count-of-messages)]
;                                (producer/publish-to-dead-queue topic-name message))]
;        (ds/replay count-of-messages)
;        (is (= (replicate count-of-messages message) (rmq/get-msg-from-instant-queue topic-name)))))))
;
;(deftest view-test
;  (testing "puts message from dead set to instant queue"))
