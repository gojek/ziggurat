(ns ziggurat.messaging.consumer-test
  (:require [clojure.test :refer :all])
  (:require [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.retry :as retry]
            [langohr.channel :as lch]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :refer [get-dead-set-messages start-subscriber* close]]
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

(defn mock-mapper-with-limit-fn [retry-counter success-tracker limit]
  "Retry for the specified limit times.
   Limit of -1 would mean we never retry."
  (fn [message]
    (cond (< @retry-counter limit)
          (do (swap! retry-counter inc)
              :retry)

          (= (:msg message) "skip")
          :skip

          :else
          (do (swap! success-tracker (constantly true))
              :success))))

(defn gen-msg [len]
  {:gen-key (apply str (take len (repeatedly #(char (+ (rand 26) 65)))))})

(defn block-until [success-fn]
  (try
    (retry/with-retry {:count 5 :wait 1000 :on-failure (fn [e] (prn "Failed. Retrying... \n"))}
      (when-not (success-fn)
        (throw (ex-info "Try failed." {}))))
    (catch Throwable e nil)))

(deftest test-retries
  (testing "when retry is enabled the mapper-fn should be retried until return success"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            success-tracker     (atom false)
            msg                 {:foo "bar"}
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly 5))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* ch (mock-mapper-with-limit-fn retry-counter success-tracker 2))

          (producer/publish-to-delay-queue msg)

          (block-until (fn [] @success-tracker))

          (is (= 2 @retry-counter))
          (is (= true @success-tracker))

          (close ch)))))

  (testing "when retry is enabled the mapper-fn should not be retried if it returns skip"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            success-tracker     (atom false)
            msg                 {:msg "skip"}
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly 5))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* ch (mock-mapper-with-limit-fn retry-counter success-tracker -1))

          (producer/publish-to-delay-queue msg)

          (block-until (fn [] @success-tracker))

          (is (= 0 @retry-counter))
          (is (= false @success-tracker))

          (close ch)))))

  (testing "when retry is enabled the mapper-fn should be retried with the maximum specified times"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            success-tracker     (atom false)
            retries             5
            no-of-msgs          1
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly retries))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* ch (mock-mapper-with-limit-fn retry-counter success-tracker 10))

          (dotimes [_ no-of-msgs]
            (producer/retry (gen-msg 10)))

          (block-until (fn [] (>= (count (get-dead-set-messages no-of-msgs false)) no-of-msgs)))

          (is (= (* (inc retries) no-of-msgs) @retry-counter))
          (is (= false @success-tracker))

          (close ch))))))
