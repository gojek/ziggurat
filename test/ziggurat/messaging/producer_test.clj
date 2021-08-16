(ns ziggurat.messaging.producer-test
  (:require [clojure.test :refer :all]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.queue :as lq]
            [ziggurat.config :refer [rabbitmq-config ziggurat-config channel-retry-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :as util]
            [ziggurat.util.rabbitmq :as rmq]
            [ziggurat.util.rabbitmq :refer [bytes-to-str]]
            [langohr.basic :as lb]
            [ziggurat.config :as config]
            [ziggurat.tracer :refer [tracer]]
            [ziggurat.middleware.default :as zmd]
            [ziggurat.metrics :as metrics])
  (:import [org.apache.kafka.common.header.internals RecordHeaders RecordHeader]
           (com.rabbitmq.client Channel Connection ShutdownSignalException AlreadyClosedException)
           (java.io IOException)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(def topic-entity :default)
(def message-payload {:message (.getBytes "hello-world") :topic-entity topic-entity :metadata {:topic "x" :partition 1 :timestamp 123}})
(defn retry-count-config [] (-> (ziggurat-config) :retry :count))

(deftest retry-for-channel-test
  (testing "message in channel will be sent to dead queue when message doesn't have retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1  #(constantly nil)}}
      (let [channel                  :channel-1
            retry-count              (atom (:count (config/channel-retry-config topic-entity channel)))
            expected-message-payload (assoc message-payload :retry-count @retry-count)]
        (producer/retry-for-channel message-payload channel)
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))

  (testing "message in channel will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)
                 :channel-1  #(constantly nil)}}
      (let [retry-count              (atom 2)
            channel                  :channel-1
            channel-retry-count      (:count (config/channel-retry-config topic-entity channel))
            retry-message-payload    (assoc message-payload :retry-count @retry-count)

            expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
        (producer/retry-for-channel retry-message-payload channel)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
            (is (= (get message-from-mq :retry-count 0) @retry-count))
            (producer/retry-for-channel message-from-mq channel)))
        (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
          (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))

  (testing "message in channel will be retried in delay queue with suffix 1 if message retry-count exceeds retry count in channel config"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router
                                                     {:default
                                                      {:channels
                                                       {:exponential-retry
                                                        {:retry {:count            5
                                                                 :enabled          true
                                                                 :type             :exponential
                                                                 :queue-timeout-ms 1000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn        #(constantly nil)
                   :exponential-retry #(constantly nil)}}
        (let [channel                  :exponential-retry
              retry-message-payload    (assoc message-payload :retry-count 10)
              expected-message-payload (assoc message-payload :retry-count 9)
              _                        (producer/retry-for-channel retry-message-payload channel)
              message-from-mq          (rmq/get-message-from-channel-retry-queue topic-entity channel 1)]
          (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))

  (testing "message in channel will be retried with linear queue timeout"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router {:default {:channels {:linear-retry {:retry {:count            5
                                                                                                                 :enabled          true
                                                                                                                 :queue-timeout-ms 2000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn   #(constantly nil)
                   :linear-retry #(constantly nil)}}
        (let [retry-count              (atom 2)
              channel                  :linear-retry
              channel-retry-count      (:count (channel-retry-config topic-entity channel))
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
          (producer/retry-for-channel retry-message-payload channel)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-message-from-channel-delay-queue topic-entity channel)]
              (is (= (get message-from-mq :retry-count 0) @retry-count))
              (producer/retry-for-channel message-from-mq channel)))
          (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
            (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq))))))))

  (testing "message in channel will be retried with exponential timeout calculated from channel specific queue-timeout-ms value"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :stream-router {:default {:channels {:exponential-retry {:retry {:count            5
                                                                                                                      :enabled          true
                                                                                                                      :type             :exponential
                                                                                                                      :queue-timeout-ms 1000}}}}}))]
      (fix/with-queues
        {:default {:handler-fn        #(constantly nil)
                   :exponential-retry #(constantly nil)}}
        (let [retry-count              (atom 5)
              channel                  :exponential-retry
              channel-retry-count      (:count (channel-retry-config topic-entity channel))
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count channel-retry-count)]
          (producer/retry-for-channel retry-message-payload channel)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-message-from-channel-retry-queue topic-entity channel (- 5 @retry-count))]
              (is (= (get message-from-mq :retry-count 0) @retry-count))
              (producer/retry-for-channel message-from-mq channel)))
          (let [message-from-mq (rmq/get-msg-from-channel-dead-queue topic-entity channel)]
            (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))))

(deftest retry-test
  (testing "message with a retry count of greater than 0 will publish to delay queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload    (assoc message-payload :retry-count 5)
            expected-message-payload (update retry-message-payload :retry-count dec)]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))

  (testing "message with a retry count of 0 will publish to dead queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload     (assoc message-payload :retry-count 0)
            expected-dead-set-message (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= (bytes-to-str expected-dead-set-message) (bytes-to-str message-from-mq)))))))

  (testing "message with no retry count will publish to dead queue"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload     message-payload
            expected-dead-set-message (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= (bytes-to-str expected-dead-set-message) (bytes-to-str message-from-mq)))))))

  (testing "message with negative retry count will not be published any where"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-message-payload (assoc message-payload :retry-count -1)]
        (producer/retry retry-message-payload)
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= nil message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
          (is (= nil message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-instant-queue "default")]
          (is (= nil message-from-mq))))))

  (testing "publish to delay queue publishes with expiration from config"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [expected-props {:content-type "application/octet-stream"
                            :persistent   true
                            :expiration   (str (get-in (rabbitmq-config) [`:delay :queue-timeout-ms]))
                            :headers      {}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue message-payload)))))

  (testing "publish to delay queue publishes with parsed record headers"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [test-message-payload (assoc message-payload :headers (RecordHeaders. (list (RecordHeader. "key" (byte-array (map byte "value"))))))
            expected-props       {:content-type "application/octet-stream"
                                  :persistent   true
                                  :expiration   (str (get-in (rabbitmq-config) [:delay :queue-timeout-ms]))
                                  :headers      {"key" "value"}}]
        (with-redefs [lb/publish (fn [_ _ _ _ props]
                                   (is (= expected-props props)))]
          (producer/publish-to-delay-queue test-message-payload)))))

  (testing "message will be retried as defined in message retry-count when message has retry-count"
    (fix/with-queues
      {:default {:handler-fn #(constantly nil)}}
      (let [retry-count              (atom 2)
            retry-message-payload    (assoc message-payload :retry-count @retry-count)
            expected-message-payload (assoc message-payload :retry-count (retry-count-config))]
        (producer/retry retry-message-payload)
        (while (> @retry-count 0)
          (swap! retry-count dec)
          (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
            (is (= (get message-from-mq :retry-count 0) @retry-count))
            (producer/retry message-from-mq)))
        (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
          (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))

  (testing "[Backward Compatiblity] Messages will be retried even if retry type is not present in the config."
    (with-redefs [ziggurat-config (constantly (update-in (ziggurat-config) [:retry] dissoc :type))]
      (fix/with-queues
        {:default {:handler-fn #(constantly nil)}}
        (let [retry-count              (atom 2)
              retry-message-payload    (assoc message-payload :retry-count @retry-count)
              expected-message-payload (assoc message-payload :retry-count (retry-count-config))]
          (producer/retry retry-message-payload)
          (while (> @retry-count 0)
            (swap! retry-count dec)
            (let [message-from-mq (rmq/get-msg-from-delay-queue "default")]
              (is (= (get message-from-mq :retry-count 0) @retry-count))
              (producer/retry message-from-mq)))
          (let [message-from-mq (rmq/get-msg-from-dead-queue "default")]
            (is (= (bytes-to-str expected-message-payload) (bytes-to-str message-from-mq)))))))))

(deftest retry-with-exponential-backoff-test
  (testing "message will publish to delay with retry count queue when exponential backoff enabled"
    (with-redefs [ziggurat-config (constantly (assoc (ziggurat-config)
                                                     :retry {:count   5
                                                             :enabled true
                                                             :type    :exponential}))]

      (testing "message with available retry counts same as that of ziggurat-config will be published to delay queue with suffix 1"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload (assoc message-payload :retry-count (get-in (ziggurat-config) [:retry :count]))
                expected-message      (assoc message-payload :retry-count 4)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 1)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message)))))))

      (testing "message with available retry counts as 4 will be published to delay queue with suffix 2"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload (assoc message-payload :retry-count 4)
                expected-message      (assoc message-payload :retry-count 3)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 2)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message)))))))

      (testing "message with available retry counts as 1 will be published to delay queue with suffix 5"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload (assoc message-payload :retry-count 1)
                expected-message      message-payload]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 5)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message)))))))

      (testing "message will be retried in delay queue with suffix 1 if message retry-count exceeds retry count in config"
        (fix/with-queues
          {:default {:handler-fn #(constantly nil)}}
          (let [retry-message-payload    (assoc message-payload :retry-count 10)
                expected-message-payload (assoc message-payload :retry-count 9)]
            (producer/retry retry-message-payload)
            (let [message-from-mq (rmq/get-message-from-retry-queue "default" 1)]
              (is (= (bytes-to-str message-from-mq) (bytes-to-str expected-message-payload))))))))))

(deftest make-queues-test
  (let [ziggurat-config (ziggurat-config)]
    (testing "When retries are enabled"
      (with-redefs [config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true
                                                                      :type    :linear}))]
        (testing "it does not create queues when stream-routes are not passed"
          (let [counter (atom 0)]
            (with-redefs [producer/create-and-bind-queue (fn
                                                           ([_ _] (swap! counter inc))
                                                           ([_ _ _] (swap! counter inc)))]
              (producer/make-queues nil)
              (producer/make-queues [])
              (is (= 0 @counter)))))

        (testing "it calls create-and-bind-queue for each queue creation and each stream-route when stream-routes are passed"
          (let [counter       (atom 0)
                stream-routes {:test  {:handler-fn #(constantly nil)}
                               :test2 {:handler-fn #(constantly nil)}}]
            (with-redefs [producer/create-and-bind-queue (fn
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
            (with-redefs [producer/create-queue           (fn [queue props _]
                                                            (swap! counter inc)
                                                            (cond
                                                              (= queue instant-queue-name) (is (empty? (:arguments props)))
                                                              (= queue delay-queue-name) (is (= {"x-dead-letter-exchange" instant-exchange-name} props))
                                                              (= queue dead-queue-name) (is (empty? (:arguments props)))))
                          producer/declare-exchange       (fn [_ _] true)
                          producer/bind-queue-to-exchange (fn [_ _ _] true)]
              (producer/make-queues stream-routes)
              (is (= (* (count stream-routes) 3) @counter)))))

        (testing "it creates queues with topic entity from stream routes"
          (with-open [ch (lch/open connection)]
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
          (with-open [ch (lch/open connection)]
            (let [stream-routes                   {:default {:handler-fn #(constantly :success)}}
                  retry-count                     (get-in ziggurat-config [:retry :count])
                  instant-queue-name              (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name           (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                  delay-queue-name                (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name             (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                  dead-queue-name                 (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name              (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                  expected-queue-status           {:message-count 0 :consumer-count 0}
                  exponential-delay-queue-name    #(util/prefixed-queue-name delay-queue-name %)
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
          (with-open [ch (lch/open connection)]
            (let [stream-routes                   {:default {:handler-fn #(constantly :success)}}
                  instant-queue-name              (util/prefixed-queue-name "default" (:queue-name (:instant (rabbitmq-config))))
                  instant-exchange-name           (util/prefixed-queue-name "default" (:exchange-name (:instant (rabbitmq-config))))
                  delay-queue-name                (util/prefixed-queue-name "default" (:queue-name (:delay (rabbitmq-config))))
                  delay-exchange-name             (util/prefixed-queue-name "default" (:exchange-name (:delay (rabbitmq-config))))
                  dead-queue-name                 (util/prefixed-queue-name "default" (:queue-name (:dead-letter (rabbitmq-config))))
                  dead-exchange-name              (util/prefixed-queue-name "default" (:exchange-name (:dead-letter (rabbitmq-config))))
                  expected-queue-status           {:message-count 0 :consumer-count 0}
                  exponential-delay-queue-name    #(util/prefixed-queue-name delay-queue-name %)
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
                stream-routes           {:default {:handler-fn #(constantly nil)}}]
            (with-redefs [config/ziggurat-config    (constantly (update-in ziggurat-config [:retry] dissoc :type))
                          producer/make-queue       (constantly nil)
                          producer/make-delay-queue (fn [topic]
                                                      (if (= topic :test)
                                                        (reset! make-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates delay queue for linear retries when retry type is incorrectly defined in the config"
          (let [make-delay-queue-called (atom false)
                stream-routes           {:default {:handler-fn #(constantly nil)}}]
            (with-redefs [config/ziggurat-config    (constantly (assoc-in ziggurat-config [:retry :type] :incorrect))
                          producer/make-queue       (constantly nil)
                          producer/make-delay-queue (fn [topic]
                                                      (if (= topic :test)
                                                        (reset! make-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates channel delay queue for linear retries when retry type is not defined in the channel config"
          (let [make-channel-delay-queue-called (atom false)
                stream-routes                   {:default {:handler-fn #(constantly nil) :channel-1 {:handler-fn #(constantly nil)}}}]
            (with-redefs [config/ziggurat-config            (constantly (update-in ziggurat-config [:stream-router :default :channels :channel-1 :retry] dissoc :type))
                          producer/make-channel-queue       (constantly nil)
                          producer/make-channel-delay-queue (fn [topic channel]
                                                              (if (and (= channel :channel-1) (= topic :default))
                                                                (reset! make-channel-delay-queue-called true)))]
              (producer/make-queues stream-routes))))
        (testing "it creates channel delay queue for linear retries when an incorrect retry type is defined in the channel config"
          (let [make-channel-delay-queue-called (atom false)
                stream-routes                   {:default {:handler-fn #(constantly nil) :channel-1 {:handler-fn #(constantly nil)}}}]
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
            (with-redefs [producer/create-and-bind-queue (fn
                                                           ([_ _] (swap! counter inc))
                                                           ([_ _ _] (swap! counter inc)))]
              (producer/make-queues {:default {:handler-fn #(constantly :success)}})
              (is (= 0 @counter)))))

        (testing "it creates queues with topic entity for channels only"
          (with-open [ch (lch/open connection)]
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
          (with-open [ch (lch/open connection)]
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

(deftest publish-to-instant-queue-test
  (testing "given a message-payload, it publishes it to the correct queue"
    (let [expected-exchange-name      "exchange"
          expected-topic-entity       topic-entity
          prefixed-queue-name-called? (atom false)
          publish-called?             (atom false)
          retry-message-payload       (assoc message-payload :retry-count 0)]
      (with-redefs [rabbitmq-config          (constantly {:instant {:exchange-name expected-exchange-name}})
                    util/prefixed-queue-name (fn [topic-entity exchange]
                                               (if (and (= topic-entity expected-topic-entity)
                                                        (= exchange expected-exchange-name))
                                                 (reset! prefixed-queue-name-called? true))
                                               expected-exchange-name)
                    producer/publish         (fn [exchange message]
                                               (if (and (= exchange expected-exchange-name)
                                                        (= message retry-message-payload))
                                                 (reset! publish-called? true)))]
        (producer/publish-to-instant-queue retry-message-payload)
        (is (true? @prefixed-queue-name-called?))
        (is (true? @publish-called?))))))

(deftest publish-behaviour-on-rabbitmq-disconnection-test
  (testing "producer/publish tries to publish again if IOException is thrown"
    (let [publish-called (atom 0)]
      (with-redefs [lch/open                (fn [_] (reify Channel (close [_] nil)))
                    lb/publish              (fn [_ _ _ _ _]
                                              (when (< @publish-called 2)
                                                (swap! publish-called inc)
                                                (throw (IOException. "io exception"))))
                    metrics/increment-count (fn [_ _ _] nil)]
        (producer/publish "random-exchange" {:topic-entity "hello"} 12345)
        (is (= 2 @publish-called)))))
  (testing "publish/producer tries to publish again if already closed exception is received"
    (let [publish-called (atom 0)]
      (with-redefs [lch/open                (fn [^Connection _] (reify Channel (close [_] nil)))
                    lb/publish              (fn [_ _ _ _ _]
                                              (when (< @publish-called 2)
                                                (swap! publish-called inc)
                                                (throw (AlreadyClosedException. (ShutdownSignalException. true true nil nil)))))
                    metrics/increment-count (fn [_ _ _] nil)]
        (producer/publish "random-exchange" {:topic-entity "hello"} 12345)
        (is (= 2 @publish-called)))))
  (testing "producer/publish does not try again if the exception thrown is neither IOException nor AlreadyClosedException"
    (let [publish-called (atom 0)]
      (with-redefs [lch/open                (fn [^Connection _] (reify Channel (close [_] nil)))
                    lb/publish              (fn [_ _ _ _ _]
                                              (when (< @publish-called 2)
                                                (swap! publish-called inc)
                                                (throw (Exception. "non-io exception"))))
                    metrics/increment-count (fn [_ _ _] nil)]
        (producer/publish "random-exchange" {:topic-entity "hello"} 12345)
        (is (= 1 @publish-called))))))

(deftest publish-to-delay-queue-test
  (testing "creates a span when tracer is enabled"
    (let [stream-routes {:default {:handler-fn #(constantly nil)
                                   :channel-1  #(constantly nil)}}]
      (.reset tracer)
      (fix/with-queues
        stream-routes
        (do
          (producer/retry message-payload)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 1 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))))))))

(deftest publish-to-channel-instant-queue-test
  (testing "creates a span when tracer is enabled"
    (let [stream-routes {:default {:handler-fn #(constantly nil)
                                   :channel-1  #(constantly nil)}}]
      (.reset tracer)
      (fix/with-queues
        stream-routes
        (do
          (producer/publish-to-channel-instant-queue :channel-1 message-payload)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 1 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))))))))

(deftest get-channel-queue-timeout-ms-test
  (let [message (assoc message-payload :retry-count 2)]
    (testing "when retries are enabled"
      (let [channel :linear-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count            5
                                                                                                                          :enabled          true
                                                                                                                          :queue-timeout-ms 2000}}}}}))]
          (is (= 2000 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))
    (testing "when exponential backoff are enabled and channel queue timeout defined"
      (let [channel :exponential-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count            5
                                                                                                                          :enabled          true
                                                                                                                          :type             :exponential
                                                                                                                          :queue-timeout-ms 1000}}}}}))]
          (is (= 7000 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))

    (testing "when exponential backoff are enabled and channel queue timeout is not defined"
      (let [channel :exponential-retry]
        (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                                :stream-router {topic-entity {:channels {channel {:retry {:count   5
                                                                                                                          :enabled true
                                                                                                                          :type    :exponential}}}}}))]
          (is (= 700 (producer/get-channel-queue-timeout-ms topic-entity channel message))))))))

(deftest get-queue-timeout-ms-test
  (testing "when exponential retries are enabled"
    (let [message (assoc message-payload :retry-count 2)]
      (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                              :retry {:enabled true
                                                                      :count   5
                                                                      :type    :exponential}))]
        (is (= 700 (producer/get-queue-timeout-ms message))))))
  (testing "when exponential retries are enabled and retry-count exceeds 25, the max possible timeouts are calculated using 25 as the retry-count"
    (let [message (assoc message-payload :retry-count 20)]
      (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                              :retry {:enabled true
                                                                      :count   50
                                                                      :type    :exponential}))]
        ;; For 25 max exponential retries, exponent comes to 25-20=5, which makes timeout = 100*(2^5-1) = 3100
        (is (= 3100 (producer/get-queue-timeout-ms message))))))
  (testing "when exponential retries are enabled with total retries as 25 and if the message has already been retried 24 times, then the queue-timeout is calculated without any failure"
    (let [message (assoc message-payload :retry-count 1)]
      (with-redefs [config/ziggurat-config (constantly (assoc (config/ziggurat-config)
                                                              :retry {:enabled true
                                                                      :count   25
                                                                      :type    :exponential}
                                                              :rabbit-mq {:delay {:queue-timeout-ms 5000}}))]
        ;; For 25 max exponential retries, exponent comes to 25-1=24, which makes timeout = 5000*(2^24-1) = 83886075000
        (is (= 83886075000 (producer/get-queue-timeout-ms message)))))))

(deftest publish-with-proto-serialization-test
  (testing "publish should deserialize a message using proto before publishing"
    (let [serialize-called (atom false)]
      (with-redefs [zmd/serialize-to-message-payload-proto (fn [_] (reset! serialize-called true))
                    lch/open                               (fn [^Connection _] (reify Channel (close [_] nil)))
                    lb/publish                             (fn [_ _ _ _ _] nil)]
        (producer/publish "exchange" message-payload)
        (is (true? @serialize-called))))))



