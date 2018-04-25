(ns ziggurat.messaging.consumer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.messaging.consumer :refer [get-dead-set-messages]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.fixtures :as fix]))

(use-fixtures :once fix/init-rabbit-mq)

(deftest get-dead-set-messages-test
  (testing "when ack is enabled, get the dead set messages and remove from dead set"
    (fix/with-clear-data
      (let [count-of-messages 10
            message {:foo "bar"}
            pushed-message (doseq [counter (range count-of-messages)]
                             (producer/publish-to-dead-queue message))
            dead-set-messages (get-dead-set-messages count-of-messages true)]
        (is (= (replicate count-of-messages message) dead-set-messages))
        (is (empty? (get-dead-set-messages count-of-messages true))))))

  (testing "when ack is disabled, get the dead set messages and not remove from dead set"
    (fix/with-clear-data
      (let [count-of-messages 10
            message {:foo "bar"}
            pushed-message (doseq [counter (range count-of-messages)]
                             (producer/publish-to-dead-queue message))
            dead-set-messages (get-dead-set-messages count-of-messages false)]
        (is (= (replicate count-of-messages message) dead-set-messages))
        (is (= (replicate count-of-messages message) (get-dead-set-messages count-of-messages false)))))))
