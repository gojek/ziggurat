(ns ziggurat.messaging.consumer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.consumer :refer [get-dead-set-messages start-subscribers]]
            [ziggurat.messaging.producer :as producer]))

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

(def mapper-retries (atom 0))

(defn mock-mapper-with-limit-fn [limit]
  (fn [message]
    (if (< @mapper-retries limit)
      (do
        (swap! mapper-retries inc)
        :retry)
      :success)))

(defn mock-mapper-without-limit-fn [message]
  (swap! mapper-retries inc)
  :retry)

(deftest test-retries
  (testing "when retry is enabled the mapper-fn should be retried until return success"
    (let [original-zig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                               (update-in [:retry :enabled] (constantly true))
                                               (update-in [:jobs :instant :worker-count] (constantly 1))))]

        (start-subscribers (mock-mapper-with-limit-fn 2))
        (producer/publish-to-delay-queue {:foo "bar"})
        (Thread/sleep 2000)
        (is (= 2 @mapper-retries))
        (reset! mapper-retries 0))))

  (testing "when retry is enabled the mapper-fn should be retried with maximum 5 times"
    (let [original-zig-config (ziggurat-config)]
      (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                               (update-in [:retry :enabled] (constantly true))
                                               (update-in [:retry :count] (constantly 5))
                                               (update-in [:jobs :instant :worker-count] (constantly 1))))]

        (start-subscribers mock-mapper-without-limit-fn)
        (producer/publish-to-delay-queue {:foo "bar"})
        (Thread/sleep 2000)
        (is (= 5 @mapper-retries))
        (reset! mapper-retries 0)))))

