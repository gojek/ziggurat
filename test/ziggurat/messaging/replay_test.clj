(ns ziggurat.messaging.replay-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.fixtures :as fix]
            [ziggurat.messaging.replay :refer [replay]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.util.rabbitmq :as rmq]))

(use-fixtures :once fix/init-rabbit-mq)

(deftest replay-test
  (testing "puts message from dead set to instant queue")
  (let [count-of-messages 10
        message {:foo "bar"}
        pushed-message (doseq [counter (range count-of-messages)]
                         (producer/publish-to-dead-queue message))]
    (replay count-of-messages)
    (is (= (replicate count-of-messages message) (rmq/get-msg-from-instant-queue)))))
