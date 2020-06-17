(ns ziggurat.messaging.rabbitmq.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :as util]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.config :as config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.config :as config]
            [ziggurat.messaging.rabbitmq-wrapper :as rmqw]
            [ziggurat.messaging.rabbitmq.producer :as rm-prod]
            [langohr.channel :as lch]
            [ziggurat.fixtures :as fix]
            [langohr.queue :as lq]
            [langohr.exchange :as le]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest make-queues-test
  (let [ziggurat-config (ziggurat-config)]
    (testing "When retries are enabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true
                                                                      :type :linear}))]
        (testing "it does not create queues when stream-routes are not passed"
          (let [counter (atom 0)]
            (with-redefs [rm-prod/create-and-bind-queue (fn
                                                       ([_ _] (swap! counter inc))
                                                       ([_ _ _] (swap! counter inc)))]
              (producer/make-queues nil)
              (producer/make-queues [])
              (is (= 0 @counter)))))

        (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
          (let [counter       (atom 0)
                stream-routes {:test  {:handler-fn #(constantly nil)}
                               :test2 {:handler-fn #(constantly nil)}}]
            (with-redefs [rm-prod/create-and-bind-queue (fn
                                                       ([_ _] (swap! counter inc))
                                                       ([_ _ _] (swap! counter inc)))]
              (producer/make-queues stream-routes)
              (is (= (* (count stream-routes) 3) @counter)))))

        (testing "it calls create queue for each queue creation with appropriate props"
          (let [counter               (atom 0)
                stream-routes         {:default {:handler-fn #(constantly nil)}}
                instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))]
            (with-redefs [rm-prod/create-queue           (fn [queue props _]
                                                        (swap! counter inc)
                                                        (cond
                                                          (= queue instant-queue-name) (is (empty? (:arguments props)))
                                                          (= queue delay-queue-name) (is (= {"x-dead-letter-exchange" instant-exchange-name} props))
                                                          (= queue dead-queue-name) (is (empty? (:arguments props)))))
                          rm-prod/declare-exchange       (fn [_ _] true)
                          rm-prod/bind-queue-to-exchange (fn [_ _ _] true)]
              (producer/make-queues stream-routes)
              (is (= (* (count stream-routes) 3) @counter)))))

        (testing "it creates queues with topic entity from stream routes"
          (with-open [ch (lch/open rmqw/connection)]
            (let [stream-routes         {:default {:handler-fn #(constantly :success)}}

                  instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))

                  delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name   (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))

                  dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name    (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))

                  expected-queue-status {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)

              (is (= expected-queue-status (lq/status ch instant-queue-name)))
              (is (= expected-queue-status (lq/status ch delay-queue-name)))
              (is (= expected-queue-status (lq/status ch dead-queue-name)))

              (lq/delete ch instant-queue-name)
              (lq/delete ch delay-queue-name)
              (lq/delete ch dead-queue-name)
              (le/delete ch delay-exchange-name)
              (le/delete ch instant-exchange-name)
              (le/delete ch dead-exchange-name))))
        (testing "it creates queues with suffixes in the range [1, retry-count] when exponential backoff is enabled"
          (with-open [ch (lch/open rmqw/connection)]
            (let [stream-routes         {:default {:handler-fn #(constantly :success)}}
                  retry-count           (get-in ziggurat-config [:retry :count])
                  instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                  delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name   (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                  dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name    (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                  expected-queue-status {:message-count 0 :consumer-count 0}
                  exponential-delay-queue-name #(util/prefixed-queue-name delay-queue-name %)
                  exponential-delay-exchange-name #(util/prefixed-queue-name delay-exchange-name %)]

              (with-redefs [config/ziggurat-config (constantly (assoc-in ziggurat-config [:retry :type] :exponential))]
                (producer/make-queues stream-routes)

                (is (= expected-queue-status (lq/status ch dead-queue-name)))
                (is (= expected-queue-status (lq/status ch instant-queue-name)))
                (lq/delete ch instant-queue-name)
                (lq/delete ch dead-queue-name)
                (le/delete ch instant-exchange-name)
                (le/delete ch dead-exchange-name)

                ;; Verifying that delay queues with appropriate suffixes have been created
                (doseq [s (range 1 (inc retry-count))]
                  (is (= expected-queue-status (lq/status ch (exponential-delay-queue-name s))))
                  (lq/delete ch (exponential-delay-queue-name s))
                  (le/delete ch (exponential-delay-exchange-name s)))))))
        (testing "it creates queues with suffixes in the range [1, 25] when exponential backoff is enabled and retry-count is more than 25"
          (with-open [ch (lch/open rmqw/connection)]
            (let [stream-routes         {:default {:handler-fn #(constantly :success)}}
                  instant-queue-name    (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                  delay-queue-name      (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name   (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                  dead-queue-name       (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name    (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                  expected-queue-status {:message-count 0 :consumer-count 0}
                  exponential-delay-queue-name #(util/prefixed-queue-name delay-queue-name %)
                  exponential-delay-exchange-name #(util/prefixed-queue-name delay-exchange-name %)]

              (with-redefs [config/ziggurat-config (constantly (-> ziggurat-config
                                                                   (assoc-in [:retry :type] :exponential)
                                                                   (assoc-in [:retry :count] 50)))]
                (producer/make-queues stream-routes)
                (is (= expected-queue-status (lq/status ch dead-queue-name)))
                (is (= expected-queue-status (lq/status ch instant-queue-name)))
                (lq/delete ch instant-queue-name)
                (lq/delete ch dead-queue-name)
                (le/delete ch instant-exchange-name)
                (le/delete ch dead-exchange-name)
                ;; Verifying that delay queues with appropriate suffixes have been created
                (doseq [s (range 1 25)]
                  (is (= expected-queue-status (lq/status ch (exponential-delay-queue-name s))))
                  (lq/delete ch (exponential-delay-queue-name s))
                  (le/delete ch (exponential-delay-exchange-name s)))))))
        (testing "it creates delay queue for linear retries when retry type is not defined in the config"
          (let [make-delay-queue-called (atom false)
                stream-routes          {:default  {:handler-fn #(constantly nil)}}]
            (with-redefs [config/ziggurat-config    (constantly (update-in ziggurat-config [:retry] dissoc :type))
                          producer/make-queue       (constantly nil)
                          producer/make-delay-queue (fn [topic]
                                                      (if (= topic :test)
                                                        (reset! make-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates delay queue for linear retries when retry type is incorrectly defined in the config"
          (let [make-delay-queue-called (atom false)
                stream-routes          {:default  {:handler-fn #(constantly nil)}}]
            (with-redefs [config/ziggurat-config    (constantly (assoc-in ziggurat-config [:retry :type] :incorrect))
                          producer/make-queue       (constantly nil)
                          producer/make-delay-queue (fn [topic]
                                                      (if (= topic :test)
                                                        (reset! make-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates channel delay queue for linear retries when retry type is not defined in the channel config"
          (let [make-channel-delay-queue-called (atom false)
                stream-routes          {:default {:handler-fn #(constantly nil) :channel-1  {:handler-fn #(constantly nil)}}}]
            (with-redefs [config/ziggurat-config            (constantly (update-in ziggurat-config [:stream-router :default :channels :channel-1 :retry] dissoc :type))
                          producer/make-channel-queue       (constantly nil)
                          producer/make-channel-delay-queue (fn [topic channel]
                                                              (if (and (= channel :channel-1) (= topic :default))
                                                                (reset! make-channel-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates channel delay queue for linear retries when an incorrect retry type is defined in the channel config"
          (let [make-channel-delay-queue-called (atom false)
                stream-routes          {:default {:handler-fn #(constantly nil) :channel-1  {:handler-fn #(constantly nil)}}}]
            (with-redefs [config/ziggurat-config            (constantly (assoc-in ziggurat-config [:stream-router :default :channels :channel-1 :retry :type] :incorrect))
                          producer/make-channel-queue       (constantly nil)
                          producer/make-channel-delay-queue (fn [topic channel]
                                                              (if (and (= channel :channel-1) (= topic :default))
                                                                (reset! make-channel-delay-queue-called true)))]
              (producer/make-queues stream-routes))))))

    (testing "when retries are disabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (testing "it does not create queues when stream-routes are not passed"
          (let [counter (atom 0)]
            (with-redefs [rm-prod/create-and-bind-queue (fn
                                                       ([_ _] (swap! counter inc))
                                                       ([_ _ _] (swap! counter inc)))]
              (producer/make-queues {:default {:handler-fn #(constantly :success)}})
              (is (= 0 @counter)))))

        (testing "it creates queues with topic entity for channels only"
          (with-open [ch (lch/open rmqw/connection)]
            (let [stream-routes                  {:default {:handler-fn #(constantly :success) :channel-1 #(constantly :success)}}
                  instant-queue-suffix           (:queue-name (:instant (rabbitmq-config)))
                  instant-exchange-suffix        (:exchange-name (:instant (rabbitmq-config)))
                  delay-queue-suffix             (:queue-name (:delay (rabbitmq-config)))
                  delay-exchange-suffix          (:exchange-name (:delay (rabbitmq-config)))
                  dead-letter-queue-suffix       (:queue-name (:dead-letter (rabbitmq-config)))
                  dead-letter-exchange-suffix    (:exchange-name (:dead-letter (rabbitmq-config)))
                  prefix-name                    "default_channel_channel-1"
                  channel1-instant-queue-name    (util/prefixed-queue-name prefix-name instant-queue-suffix)
                  channel1-instant-exchange-name (util/prefixed-queue-name prefix-name instant-exchange-suffix)
                  channel1-delay-queue-name      (util/prefixed-queue-name prefix-name delay-queue-suffix)
                  channel1-delay-exchange-name   (util/prefixed-queue-name prefix-name delay-exchange-suffix)
                  channel1-dead-queue-name       (util/prefixed-queue-name prefix-name dead-letter-queue-suffix)
                  channel1-dead-exchange-name    (util/prefixed-queue-name prefix-name dead-letter-exchange-suffix)
                  expected-queue-status          {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)
              (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
              (is (= expected-queue-status (lq/status ch channel1-delay-queue-name)))
              (is (= expected-queue-status (lq/status ch channel1-dead-queue-name)))

              (lq/delete ch channel1-instant-queue-name)
              (lq/delete ch channel1-delay-queue-name)
              (lq/delete ch channel1-dead-queue-name)
              (le/delete ch channel1-delay-exchange-name)
              (le/delete ch channel1-instant-exchange-name)
              (le/delete ch channel1-dead-exchange-name))))))

    (testing "when retries are disabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}
                                                              :stream-router {:default {:channels {:channel-1 {:retry {:enabled false}}}}}))]

        (testing "it creates instant queues with topic entity for channels only"
          (with-open [ch (lch/open rmqw/connection)]
            (let [stream-routes                  {:default {:handler-fn #(constantly :success) :channel-1 #(constantly :success)}}
                  instant-queue-suffix           (:queue-name (:instant (rabbitmq-config)))
                  instant-exchange-suffix        (:exchange-name (:instant (rabbitmq-config)))
                  prefix-name                    "default_channel_channel-1"
                  channel1-instant-queue-name    (util/prefixed-queue-name prefix-name instant-queue-suffix)
                  channel1-instant-exchange-name (util/prefixed-queue-name prefix-name instant-exchange-suffix)
                  expected-queue-status          {:message-count 0 :consumer-count 0}]

              (producer/make-queues stream-routes)
              (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
              (lq/delete ch channel1-instant-queue-name)
              (le/delete ch channel1-instant-exchange-name))))))))
