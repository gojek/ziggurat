(ns ziggurat.messaging.queues-test
  (:require [ziggurat.messaging.queues :as q :refer [queues]]
            [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [rabbitmq-config ziggurat-config channel-retry-config]]
            [ziggurat.messaging.connection :refer [producer-connection]]
            [ziggurat.messaging.producer]
            [ziggurat.messaging.util :as util :refer :all]
            [ziggurat.config :as config]
            [mount.core :as mount]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.message-payload :refer [->MessagePayload]]
            [langohr.channel :as lch]
            [clojure.tools.logging :as log]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(deftest make-queues-test-when-retries-are-enabled
  (let [ziggurat-config (ziggurat-config)]
    (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                       :retry {:enabled true
                                                               :type    :linear}))]
      (testing "it does not create queues when stream-routes are not passed"
        (let [counter (atom 0)]
          (with-redefs [q/create-and-bind-queue (fn
                                                         ([_ _] (swap! counter inc))
                                                         ([_ _ _] (swap! counter inc)))]
            (-> (mount/only [#'queues])
                (mount/with-args nil)
                (mount/start))
            (mount/stop #'queues)
            (-> (mount/only [#'queues])
                (mount/with-args [])
                (mount/start))
            (mount/stop #'queues)
            (is (= 0 @counter)))))

      (testing "it calls create queue for each queue creation with appropriate props"
        (let [counter (atom 0)
              stream-routes {:default {:handler-fn #(constantly nil)}}
              instant-queue-name (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
              instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
              delay-queue-name (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
              dead-queue-name (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))]
          (with-redefs [q/create-queue (fn [queue props _]
                                              (swap! counter inc)
                                              (cond
                                                (= queue instant-queue-name) (is (empty? (:arguments props)))
                                                (= queue delay-queue-name) (is (= {"x-dead-letter-exchange" instant-exchange-name} props))
                                                (= queue dead-queue-name) (is (empty? (:arguments props)))))
                        q/declare-exchange (fn [_ _] true)
                        q/bind-queue-to-exchange (fn [_ _ _] true)]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (is (= (* (count stream-routes) 3) @counter))
            (mount/stop #'queues))))

      (testing "it creates queues with topic entity from stream routes"
        (with-open [ch (lch/open producer-connection)]
          (let [stream-routes {:default {:handler-fn #(constantly :success)}}

                instant-queue-name (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))

                delay-queue-name (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                delay-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))

                dead-queue-name (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                dead-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))

                expected-queue-status {:message-count 0 :consumer-count 0}]

            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))

            (is (= expected-queue-status (lq/status ch instant-queue-name)))
            (is (= expected-queue-status (lq/status ch delay-queue-name)))
            (is (= expected-queue-status (lq/status ch dead-queue-name)))

            (lq/delete ch instant-queue-name)
            (lq/delete ch delay-queue-name)
            (lq/delete ch dead-queue-name)
            (le/delete ch delay-exchange-name)
            (le/delete ch instant-exchange-name)
            (le/delete ch dead-exchange-name)

            (mount/stop #'queues))))

      (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
        (let [counter (atom 0)
              stream-routes {:test  {:handler-fn #(constantly nil)}
                             :test2 {:handler-fn #(constantly nil)}}]
          (with-redefs [q/create-and-bind-queue (fn
                                                       ([_ _] (swap! counter inc))
                                                       ([_ _ _] (swap! counter inc)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (is (= (* (count stream-routes) 3) @counter))
            (mount/stop #'queues))))

      (testing "it creates queues with suffixes in the range [1, 25] when exponential backoff is enabled and retry-count is more than 25"
        (with-open [ch (lch/open producer-connection)]
          (let [stream-routes {:default {:handler-fn #(constantly :success)}}
                instant-queue-name (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                delay-queue-name (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                delay-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                dead-queue-name (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                dead-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                expected-queue-status {:message-count 0 :consumer-count 0}
                exponential-delay-queue-name #(util/prefixed-queue-name delay-queue-name %)
                exponential-delay-exchange-name #(util/prefixed-queue-name delay-exchange-name %)]

            (with-redefs [config/ziggurat-config (constantly (-> ziggurat-config
                                                                 (assoc-in [:retry :type] :exponential)
                                                                 (assoc-in [:retry :count] 50)))]
              (-> (mount/only [#'queues])
                  (mount/with-args {:stream-routes stream-routes})
                  (mount/start))
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
                (le/delete ch (exponential-delay-exchange-name s)))
              (mount/stop #'queues)))))

      (testing "it creates delay queue for linear retries when retry type is not defined in the config"
        (let [make-delay-queue-called (atom false)
              stream-routes {:default {:handler-fn #(constantly nil)}}]
          (with-redefs [config/ziggurat-config (constantly (update-in ziggurat-config [:retry] dissoc :type))
                        q/make-queue (constantly nil)
                        q/make-delay-queue (fn [topic]
                                                  (if (= topic :test)
                                                    (reset! make-delay-queue-called true)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (mount/stop #'queues))))

      (testing "it creates delay queue for linear retries when retry type is incorrectly defined in the config"
        (let [make-delay-queue-called (atom false)
              stream-routes {:default {:handler-fn #(constantly nil)}}]
          (with-redefs [config/ziggurat-config (constantly (assoc-in ziggurat-config [:retry :type] :incorrect))
                        q/make-queue (constantly nil)
                        q/make-delay-queue (fn [topic]
                                                  (if (= topic :test)
                                                    (reset! make-delay-queue-called true)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (mount/stop #'queues))))

      (testing "it creates channel delay queue for linear retries when retry type is not defined in the channel config"
        (let [make-channel-delay-queue-called (atom false)
              stream-routes {:default {:handler-fn #(constantly nil) :channel-1 {:handler-fn #(constantly nil)}}}]
          (with-redefs [config/ziggurat-config (constantly (update-in ziggurat-config [:stream-router :default :channels :channel-1 :retry] dissoc :type))
                        q/make-channel-queue (constantly nil)
                        q/make-channel-delay-queue (fn [topic channel]
                                                          (if (and (= channel :channel-1) (= topic :default))
                                                            (reset! make-channel-delay-queue-called true)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (mount/stop #'queues))))

      (testing "it creates channel delay queue for linear retries when an incorrect retry type is defined in the channel config"
        (let [make-channel-delay-queue-called (atom false)
              stream-routes {:default {:handler-fn #(constantly nil) :channel-1 {:handler-fn #(constantly nil)}}}]
          (with-redefs [config/ziggurat-config (constantly (assoc-in ziggurat-config [:stream-router :default :channels :channel-1 :retry :type] :incorrect))
                        q/make-channel-queue (constantly nil)
                        q/make-channel-delay-queue (fn [topic channel]
                                                          (if (and (= channel :channel-1) (= topic :default))
                                                            (reset! make-channel-delay-queue-called true)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes stream-routes})
                (mount/start))
            (mount/stop #'queues))))

      (testing "it creates queues with suffixes in the range [1, retry-count] when exponential backoff is enabled"
        (with-open [ch (lch/open producer-connection)]
          (let [stream-routes {:default {:handler-fn #(constantly :success)}}
                retry-count (get-in ziggurat-config [:retry :count])
                instant-queue-name (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                instant-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                delay-queue-name (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                delay-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                dead-queue-name (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                dead-exchange-name (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                expected-queue-status {:message-count 0 :consumer-count 0}
                exponential-delay-queue-name #(util/prefixed-queue-name delay-queue-name %)
                exponential-delay-exchange-name #(util/prefixed-queue-name delay-exchange-name %)]

            (with-redefs [config/ziggurat-config (constantly (assoc-in ziggurat-config [:retry :type] :exponential))]
              (-> (mount/only [#'queues])
                  (mount/with-args {:stream-routes stream-routes})
                  (mount/start))

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
                (le/delete ch (exponential-delay-exchange-name s)))

              (mount/stop #'queues))))))))

(deftest make-queues-test-when-retries-are-disabled
  (let [ziggurat-config (ziggurat-config)]
    (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                       :retry {:enabled false}))]
      (testing "it does not create queues when stream-routes are not passed"
        (let [counter (atom 0)]
          (with-redefs [q/create-and-bind-queue (fn
                                                         ([_ _] (swap! counter inc))
                                                         ([_ _ _] (swap! counter inc)))]
            (-> (mount/only [#'queues])
                (mount/with-args {:stream-routes {:default {:handler-fn #(constantly :success)}}})
                (mount/start))
            (is (= 0 @counter))
            (mount/stop #'queues))))

      (testing "it creates queues with topic entity for channels only"
        (with-open [ch (lch/open producer-connection)]
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

            (q/make-queues stream-routes)
            (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
            (is (= expected-queue-status (lq/status ch channel1-delay-queue-name)))
            (is (= expected-queue-status (lq/status ch channel1-dead-queue-name)))

            (lq/delete ch channel1-instant-queue-name)
            (lq/delete ch channel1-delay-queue-name)
            (lq/delete ch channel1-dead-queue-name)
            (le/delete ch channel1-delay-exchange-name)
            (le/delete ch channel1-instant-exchange-name)
            (le/delete ch channel1-dead-exchange-name)
            (mount/stop #'queues))))

      (testing "it creates instant queues with topic entity for channels only"
        (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                           :stream-router {:default {:channels {:channel-1 {:retry {:enabled false}}}}}))]
          (with-open [ch (lch/open producer-connection)]
            (let [stream-routes                  {:default {:handler-fn #(constantly :success) :channel-1 #(constantly :success)}}
                  instant-queue-suffix           (:queue-name (:instant (rabbitmq-config)))
                  instant-exchange-suffix        (:exchange-name (:instant (rabbitmq-config)))
                  prefix-name                    "default_channel_channel-1"
                  channel1-instant-queue-name    (util/prefixed-queue-name prefix-name instant-queue-suffix)
                  channel1-instant-exchange-name (util/prefixed-queue-name prefix-name instant-exchange-suffix)
                  expected-queue-status          {:message-count 0 :consumer-count 0}]

              (-> (mount/only [#'queues])
                  (mount/with-args {:stream-routes stream-routes})
                  (mount/start))
              (is (= expected-queue-status (lq/status ch channel1-instant-queue-name)))
              (lq/delete ch channel1-instant-queue-name)
              (le/delete ch channel1-instant-exchange-name)
              (mount/stop #'queues))))))))