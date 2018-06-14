(ns ziggurat.messaging.consumer-test
  (:require [clojure.test :refer :all])
  (:require [langohr.channel :as lch]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :refer [get-dead-set-messages start-subscriber* close get-queue-name start-subscribers]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.retry :as retry]))

(use-fixtures :once fix/init-rabbit-mq)

(defn- gen-msg []
  {:gen-key (apply str (take 10 (repeatedly #(char (+ (rand 26) 65)))))})

(deftest get-dead-set-messages-test
  (testing "when ack is enabled, get the dead set messages and remove from dead set"
    (fix/with-clear-data
      (let [count-of-messages 10
            message           (gen-msg)
            topic-identifier  "booking"
            pushed-message    (doseq [_ (range count-of-messages)]
                                (producer/publish-to-dead-queue topic-identifier message))
            dead-set-messages (get-dead-set-messages true topic-identifier count-of-messages)]
        (is (= (replicate count-of-messages message) dead-set-messages))
        (is (empty? (get-dead-set-messages true topic-identifier count-of-messages))))))

  (testing "when ack is disabled, get the dead set messages and not remove from dead set"
    (fix/with-clear-data
      (let [count-of-messages 10
            message           (gen-msg)
            topic-identifier  "booking"
            pushed-message    (doseq [_ (range count-of-messages)]
                                (producer/publish-to-dead-queue topic-identifier message))
            dead-set-messages (get-dead-set-messages false topic-identifier count-of-messages)]
        (is (= (replicate count-of-messages message) dead-set-messages))
        (is (= (replicate count-of-messages message) (get-dead-set-messages false topic-identifier count-of-messages)))))))

(defn- mock-mapper-fn [{:keys [retry-counter-atom
                               retry-limit
                               skip-promise
                               success-promise] :as opts}]
  (fn [message]
    (cond (< @retry-counter-atom (or retry-limit 5))
          (do (when retry-counter-atom (swap! retry-counter-atom inc))
              :retry)

          (= (:msg message) "skip")
          (do (when skip-promise (deliver skip-promise true))
              :skip)

          :else
          (do (when success-promise (deliver success-promise true))
              :success))))

(defn- block-and-retry-until [success-fn]
  (try
    (retry/with-retry {:count 5 :wait 1000} (success-fn))
    (catch Throwable e
      (println (.getMessage e)))))

(deftest test-retries
  (testing "when retry is enabled the mapper-fn should be retried until return success"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            success-promise     (promise)
            msg                 (gen-msg)
            topic-identifier    "booking"
            original-zig-config (ziggurat-config)
            rmq-ch              (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly 5))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* rmq-ch (mock-mapper-fn {:retry-counter-atom retry-counter
                                                     :retry-limit        2
                                                     :success-promise    success-promise}) topic-identifier)

          (producer/publish-to-delay-queue topic-identifier msg)

          (when-let [promise-success? (deref success-promise 5000 :timeout)]
            (is (not (= :timeout promise-success?)))
            (is (= true promise-success?))
            (is (= 2 @retry-counter)))

          (close rmq-ch)))))

  (testing "when retry is enabled the mapper-fn should not be retried if it returns skip"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            skip-promise        (promise)
            msg                 (assoc (gen-msg) :msg "skip")
            topic-identifier    "booking"
            original-zig-config (ziggurat-config)
            rmq-ch              (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly 5))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* rmq-ch (mock-mapper-fn {:retry-counter-atom retry-counter
                                                     :skip-promise       skip-promise
                                                     :retry-limit        -1}) topic-identifier)

          (producer/publish-to-delay-queue topic-identifier msg)

          (when-let [promise-success? (deref skip-promise 5000 :timeout)]
            (is (not (= :timeout promise-success?)))
            (is (= true promise-success?))
            (is (= 0 @retry-counter)))

          (close rmq-ch)))))

  (testing "when retry is enabled the mapper-fn should be retried with the maximum specified times"
    (fix/with-clear-data
      (let [retry-counter       (atom 0)
            retries             5
            no-of-msgs          2
            topic-identifier    "booking"
            original-zig-config (ziggurat-config)
            rmq-ch              (lch/open connection)]

        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly retries))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (start-subscriber* rmq-ch (mock-mapper-fn {:retry-counter-atom retry-counter
                                                     :retry-limit        (* no-of-msgs 10)}) topic-identifier)

          (dotimes [_ no-of-msgs]
            (producer/retry (gen-msg) topic-identifier))

          (block-and-retry-until (fn []
                                   (let [dead-set-msgs (count (get-dead-set-messages false topic-identifier no-of-msgs))]
                                     (if (< dead-set-msgs no-of-msgs)
                                       (throw (ex-info "Dead set messages were never populated"
                                                       {:dead-set-msgs dead-set-msgs}))))))

          (is (= (* (inc retries) no-of-msgs) @retry-counter))
          (is (= no-of-msgs (count (get-dead-set-messages false topic-identifier no-of-msgs)))))
        (close rmq-ch))))

  (testing "start subscribers should not call start-subscriber* when stream router is nil"
    (fix/with-clear-data
      (let [no-of-workers       3
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)
            counter             (atom 0)]

        (with-redefs [ziggurat-config   (fn [] (-> original-zig-config
                                                   (update-in [:retry :enabled] (constantly true))
                                                   (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                      start-subscriber* (fn [_ _ _] (swap! counter inc))]

          (start-subscribers nil)

          (is (= 0 @counter))
          (close ch)))))

  (testing "start subscribers should call start-subscriber* according to the product of worker and mapper-fns in stream-routes"
    (let [no-of-workers       3
          original-zig-config (ziggurat-config)
          ch                  (lch/open connection)
          counter             (atom 0)
          stream-routes       [{:booking {:handler-fn #(constantly nil)}} {:test {:handler-fn #(constantly nil)}}]]

      (with-redefs [ziggurat-config   (fn [] (-> original-zig-config
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                    start-subscriber* (fn [_ _ _] (swap! counter inc))]

        (start-subscribers stream-routes)

        (is (= (* (count stream-routes) no-of-workers) @counter))
        (close ch)))))