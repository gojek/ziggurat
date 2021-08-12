(ns ziggurat.messaging.consumer-test
  (:require [clojure.test :refer [deftest is join-fixtures testing use-fixtures]])
  (:require [langohr.basic :as lb]
            [langohr.channel :as lch]
            [taoensso.nippy :as nippy]
            [ziggurat.config :refer [ziggurat-config rabbitmq-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.messaging.connection :refer [connection]]
            [ziggurat.messaging.consumer :as consumer]
            [ziggurat.messaging.producer :as producer]
            [ziggurat.messaging.util :refer [prefixed-queue-name]]
            [ziggurat.tracer :refer [tracer]]

            [ziggurat.util.rabbitmq :as util :refer [bytes-to-str]]
            [ziggurat.message-payload :as zmp]
            [ziggurat.mapper :as mpr]
            [ziggurat.middleware.default :as zmd]
            [protobuf.core :as proto]
            [ziggurat.util.error :refer [report-error]])
  (:import (com.gojek.test.proto Example$Photo)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging
                                    fix/mount-metrics]))

(defn- gen-message-payload [topic-entity retry-count]
  {:message      (.getBytes "hello-world")
   :topic-entity topic-entity
   :retry-count  retry-count})

(def topic-entity :default)

(deftest process-dead-set-messages-test
  (let [message-payload   (gen-message-payload topic-entity 4)]
    (testing "it maps the process-message-from-queue over all the messages fetched from the queue for a topic"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)}}
        (let [count             5
              process-fn-called (atom 0)
              processing-fn     (fn [message]
                                  (when (= (bytes-to-str message) (bytes-to-str message-payload))
                                    (swap! process-fn-called inc)))
              _                 (doseq [_ (range count)]
                                  (producer/publish-to-dead-queue message-payload))]
          (consumer/process-dead-set-messages topic-entity count processing-fn)
          (is (= count @process-fn-called))
          (is (empty? (consumer/get-dead-set-messages topic-entity count))))))
    (testing "it maps the process-message-from-queue over all the messages fetched from the queue for a channel"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)
                                      :channel-1  (constantly nil)}}
        (let [count             5
              channel           "channel-1"
              process-fn-called (atom 0)
              processing-fn     (fn [message]
                                  (when (= (bytes-to-str message) (bytes-to-str message-payload))
                                    (swap! process-fn-called inc)))
              _                 (doseq [_ (range count)]
                                  (producer/publish-to-channel-dead-queue channel message-payload))]
          (consumer/process-dead-set-messages topic-entity channel count processing-fn)
          (is (= count @process-fn-called))
          (is (empty? (consumer/get-dead-set-messages topic-entity channel count))))))))

(deftest get-dead-set-messages-test
  (let [message-payload            (gen-message-payload topic-entity 5)
        comparable-message-payload (bytes-to-str message-payload)]
    (testing "get the dead set messages from dead set queue and don't pop the messages from the queue"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)}}
        (let [count-of-messages 10
              _                 (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-dead-queue message-payload))
              dead-set-messages           (consumer/get-dead-set-messages topic-entity count-of-messages)
              comparable-deadset-messages (map bytes-to-str dead-set-messages)]
          (is (= (repeat count-of-messages comparable-message-payload) comparable-deadset-messages))

                         ;; dead-set messages fetched again to prove that they don't get lost on first read
          (is (= (repeat count-of-messages comparable-message-payload) (map bytes-to-str (consumer/get-dead-set-messages topic-entity count-of-messages)))))))

    (testing "get the dead set messages from a channel dead set queue and don't pop the messages from the queue"
      (fix/with-queues {topic-entity {:handler-fn (constantly nil)
                                      :channel-1  (constantly nil)}}
        (let [count-of-messages 10
              channel           "channel-1"
              _                 (doseq [_ (range count-of-messages)]
                                  (producer/publish-to-channel-dead-queue channel message-payload))
              dead-set-messages           (consumer/get-dead-set-messages topic-entity channel count-of-messages)
              comparable-deadset-messages (map bytes-to-str dead-set-messages)]
          (is (= (repeat count-of-messages comparable-message-payload) comparable-deadset-messages))

                         ;; dead-set messages fetched again to prove that they don't get lost on first read
          (is (= (repeat count-of-messages comparable-message-payload) (map bytes-to-str (consumer/get-dead-set-messages topic-entity channel count-of-messages)))))))))

(defn- mock-mapper-fn [{:keys [retry-counter-atom call-counter-atom retry-limit skip-promise success-promise]}]
  (fn [message]
    (swap! call-counter-atom inc)
    (cond (< @retry-counter-atom (or retry-limit 5)) (do (when retry-counter-atom (swap! retry-counter-atom inc))
                                                         :retry)
          (= (:msg message) "skip")                  (do (when skip-promise (deliver skip-promise true))
                                                         :skip)
          :else                                      (do (when success-promise (deliver success-promise true))
                                                         :success))))

(deftest start-subscribers-test
  (testing "start subscribers should not be called if none of the stream-routes or batch-routes are provided"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [no-of-workers       3
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)
            counter             (atom 0)]
        (with-redefs [ziggurat-config                  (fn [] (-> original-zig-config
                                                                  (update-in [:retry :enabled] (constantly true))
                                                                  (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                      consumer/start-retry-subscriber* (fn [_ _]
                                                         (swap! counter inc))]
          (consumer/start-subscribers nil nil)
          (is (= 0 @counter))
          (util/close ch)))))

  (testing "start subscribers should call start-subscriber* according to the product of worker and mapper-fns in stream-routes"
    (let [no-of-workers       3
          original-zig-config (ziggurat-config)
          ch                  (lch/open connection)
          counter             (atom 0)
          stream-routes       {topic-entity {:handler-fn #(constantly nil)}
                               :test    {:handler-fn #(constantly nil)}}]
      (with-redefs [ziggurat-config         (fn [] (-> original-zig-config
                                                       (update-in [:retry :enabled] (constantly true))
                                                       (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                    consumer/start-retry-subscriber* (fn [_ _] (swap! counter inc))]
        (consumer/start-subscribers stream-routes {})
        (is (= (count stream-routes) @counter))
        (util/close ch))))

  (testing "start subscribers should only call start-subscriber* for batch-routes if stream routes are nil"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [no-of-workers       3
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)
            counter             (atom 0)]

        (with-redefs [ziggurat-config                  (fn [] (-> original-zig-config
                                                                  (update-in [:retry :enabled] (constantly true))
                                                                  (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                      consumer/start-retry-subscriber* (fn [_ topic-entity]
                                                         (swap! counter inc)
                                                         (is (= topic-entity :consumer-1)))]
          (consumer/start-subscribers nil {:consumer-1 {:handler-fn #()}})
          (is (= 1 @counter))
          (util/close ch)))))

  (testing "start subscribers should only call start-subscriber* for both batch-routes and stream-routes if both are provided"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [no-of-workers       3
            original-zig-config (ziggurat-config)
            ch                  (lch/open connection)
            counter             (atom 0)]

        (with-redefs [ziggurat-config                  (fn [] (-> original-zig-config
                                                                  (update-in [:retry :enabled] (constantly true))
                                                                  (update-in [:jobs :instant :worker-count] (constantly no-of-workers))))
                      consumer/start-retry-subscriber* (fn [_ topic-entity]
                                                         (swap! counter inc)
                                                         (is (or (= topic-entity :consumer-1) (= topic-entity :default))))]
          (consumer/start-subscribers {:default {:handler-fn #()}} {:consumer-1 {:handler-fn #()}})
          (is (= 2 @counter))
          (util/close ch))))))

(deftest start-channels-subscriber-test
  (testing "the mapper-fn for channel subscriber should be retried until return success when retry is enabled for that channel"
    (let [retry-counter       (atom 0)
          call-counter        (atom 0)
          success-promise     (promise)
          retry-count         5
          message-payload     (gen-message-payload topic-entity retry-count)
          channel             :channel-1
          channel-fn          (mock-mapper-fn {:retry-counter-atom retry-counter
                                               :call-counter-atom  call-counter
                                               :retry-limit        2
                                               :success-promise    success-promise})
          original-zig-config (ziggurat-config)
          rmq-ch              (lch/open connection)]
      (fix/with-queues {topic-entity {:handler-fn #(constantly nil)
                                      channel     channel-fn}}
        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:stream-router topic-entity :channels channel :retry :count] (constantly retry-count))
                                                 (update-in [:stream-router topic-entity :channels channel :retry :enabled] (constantly true))
                                                 (update-in [:stream-router topic-entity :channels channel :worker-count] (constantly 1))))]
          (with-redefs [lch/open (fn [_] rmq-ch)]
            (consumer/start-channels-subscriber {channel channel-fn} topic-entity))
          (producer/retry-for-channel message-payload channel)
          (when-let [promise-success? (deref success-promise 5000 :timeout)]
            (is (not (= :timeout promise-success?)))
            (is (= true promise-success?))
            (is (= 2 @retry-counter)))
          (util/close rmq-ch)))))

  (testing "the mapper-fn for channel subscriber should not enqueue the message when retry is disabled for that channel"
    (let [retry-counter       (atom 0)
          call-counter        (atom 0)
          success-promise     (promise)
          message-payload     (gen-message-payload topic-entity 2)
          channel             :channel-1
          channel-fn          (mock-mapper-fn {:retry-counter-atom retry-counter
                                               :call-counter-atom  call-counter
                                               :retry-limit        2
                                               :success-promise    success-promise})
          original-zig-config (ziggurat-config)
          rmq-ch              (lch/open connection)]
      (fix/with-queues {topic-entity {:handler-fn #(constantly nil)
                                      channel     channel-fn}}
        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:stream-router topic-entity :channels channel :retry :enabled] (constantly false))
                                                 (update-in [:stream-router topic-entity :channels channel :worker-count] (constantly 1))))]
          (consumer/start-channels-subscriber {channel channel-fn} topic-entity)
          (producer/publish-to-channel-instant-queue channel message-payload)
          (deref success-promise 5000 :timeout)
          (is (= 1 @call-counter))
          (util/close rmq-ch))))))

(deftest start-retry-subscriber-test
  (testing "creates a span when tracer is enabled"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [retry-counter (atom 0)
            call-counter (atom 0)
            success-promise (promise)
            retry-count 3
            message-payload (gen-message-payload topic-entity 3)
            original-zig-config (ziggurat-config)
            rmq-ch              (lch/open connection)]
        (.reset tracer)
        (with-redefs [ziggurat-config (fn [] (-> original-zig-config
                                                 (update-in [:retry :count] (constantly retry-count))
                                                 (update-in [:retry :enabled] (constantly true))
                                                 (update-in [:jobs :instant :worker-count] (constantly 1))))]

          (consumer/start-retry-subscriber* (mock-mapper-fn {:retry-counter-atom retry-counter
                                                             :call-counter-atom  call-counter
                                                             :retry-limit        0
                                                             :success-promise    success-promise}) topic-entity)

          (producer/publish-to-delay-queue message-payload)
          (when-let [promise-success? (deref success-promise 5000 :timeout)]
            (is (not (= :timeout promise-success?)))
            (is (= true promise-success?)))
          (util/close rmq-ch)
          (Thread/sleep 500)
          (let [finished-spans (.finishedSpans tracer)]
            (is (= 2 (.size finished-spans)))
            (is (= "send" (-> finished-spans
                              (.get 0)
                              (.operationName))))
            (is (= "receive" (-> finished-spans
                                 (.get 1)
                                 (.operationName))))))))))

(deftest process-message-test
  (testing "process-message function should ack message after once processing finishes"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity 1)
            processing-fn (fn [message-arg]
                            (is (= (bytes-to-str message-arg) (bytes-to-str message))))
            topic-entity-name (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-open [ch (lch/open connection)]
          (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                prefixed-queue-name (str topic-entity-name "_" queue-name)
                [meta payload]      (lb/get ch prefixed-queue-name false)
                _                   (consumer/process-message-from-queue ch meta payload topic-entity processing-fn)
                consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
            (is (= consumed-message nil)))))))
  (testing "process-message function not process a message if convert-message returns nil"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity 0)
            processing-fn-called (atom false)
            processing-fn        (fn [message-arg]
                                   (when (nil? message-arg)
                                     (reset! processing-fn-called true)))
            topic-entity-name    (name topic-entity)]
        (producer/publish-to-dead-queue message)
        (with-redefs [consumer/convert-and-ack-message (fn [_ _ _ _ _] nil)]
          (with-open [ch (lch/open connection)]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload]      (lb/get ch prefixed-queue-name false)
                  _                   (consumer/process-message-from-queue ch meta payload topic-entity processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= false @processing-fn-called))
              (is (= consumed-message nil))))))))
  (testing "process-message function should reject and re-queue a message if processing fails. It should also report the error"
    (fix/with-queues {topic-entity {:handler-fn #(constantly nil)}}
      (let [message       (gen-message-payload topic-entity 1)
            processing-fn (fn [message-arg]
                            (is (= (bytes-to-str message-arg) (bytes-to-str message)))
                            (throw (Exception. "exception message")))
            topic-entity-name (name topic-entity)
            report-fn-called? (atom false)]
        (with-redefs [report-error (fn [_ _] (reset! report-fn-called? true))]
          (producer/publish-to-dead-queue message)
          (with-open [ch (lch/open connection)]
            (let [queue-name          (get-in (rabbitmq-config) [:dead-letter :queue-name])
                  prefixed-queue-name (str topic-entity-name "_" queue-name)
                  [meta payload]      (lb/get ch prefixed-queue-name false)
                  _                   (consumer/process-message-from-queue ch meta payload topic-entity processing-fn)
                  consumed-message    (util/get-msg-from-dead-queue-without-ack topic-entity-name)]
              (is (= (bytes-to-str consumed-message) (bytes-to-str message)))
              (is @report-fn-called?))))))))

(deftest convert-and-ack-message-test
  (testing "should call publish to dead set when nippy/thaw throws an exception"
    (let [freezed-message    (nippy/freeze {:foo "bar"})
          is-publish-called? (atom false)
          topic-entity       "default"
          expected-exchange  (prefixed-queue-name topic-entity (:exchange-name (:dead-letter (rabbitmq-config))))]
      (with-redefs [nippy/thaw (fn [_] (throw (Exception. "nippy/thaw exception")))
                    lb/publish (fn [_ exchange _ payload]
                                 (is (= exchange expected-exchange))
                                 (is (= freezed-message payload))
                                 (reset! is-publish-called? true))]
        (consumer/convert-and-ack-message nil {:delivery-tag "delivery-tag"} freezed-message false topic-entity))
      (is (= @is-publish-called? true))))
  (testing "should call reject when both nippy/thaw and lb/publish throws an exception"
    (let [freezed-message   (nippy/freeze {:foo "bar"})
          is-reject-called? (atom false)]
      (with-redefs [nippy/thaw              (fn [_] (throw (Exception. "nippy/thaw exception")))
                    lb/publish              (fn [_ _ _ _]
                                              (throw (Exception. "lb/publish exception")))
                    consumer/reject-message (fn [_ _]
                                              (reset! is-reject-called? true))]
        (consumer/convert-and-ack-message nil {:delivery-tag "delivery-tag"} freezed-message false "default"))
      (is (= @is-reject-called? true)))))

(deftest convert-and-ack-message-further-tests
  (let [message                    {:id 7 :path "/photos/h2k3j4h9h23"}
        proto-class                Example$Photo
        proto-message              (proto/->bytes (proto/create proto-class message))
        message-payload            {:message proto-message :topic-entity topic-entity :retry-count 3}]
    (testing "should return deserialized message-payload if serialized using protobuf"
      (let [proto-serialized-message-payload (zmd/serialize-to-message-payload-proto message-payload)
            converted-message-payload        (consumer/convert-and-ack-message nil {:delivery-tag 1} proto-serialized-message-payload false "default")]
        (is (= (bytes-to-str converted-message-payload) (bytes-to-str message-payload)))))
    (testing "should return deserialized message-payload with topic-entity as the keyword "
      (let [proto-serialized-message-payload (zmd/serialize-to-message-payload-proto message-payload)
            converted-message-payload        (consumer/convert-and-ack-message nil {:delivery-tag 1} proto-serialized-message-payload false "default")]
        (is (keyword? (:topic-entity converted-message-payload)))
        (is (= (bytes-to-str converted-message-payload) (bytes-to-str message-payload)))))
    (testing "should return deserialized message-payload with the message as a byte array "
      (let [proto-serialized-message-payload (zmd/serialize-to-message-payload-proto message-payload)
            converted-message-payload        (consumer/convert-and-ack-message nil {:delivery-tag 1} proto-serialized-message-payload false "default")]
        (is (= "class [B" (str (type (:message converted-message-payload)))))
        (is (= (bytes-to-str converted-message-payload) (bytes-to-str message-payload)))))
    (testing "should return a ziggurat.message_payload/->MessagePayload if serialized using nippy"
      (let [expected-message-payload         (assoc (zmp/->MessagePayload proto-message topic-entity) :retry-count 4)
            nippy-serialized-message-payload (nippy/freeze expected-message-payload)
            converted-message-payload        (consumer/convert-and-ack-message nil {:delivery-tag 1} nippy-serialized-message-payload false "default")]
        (is (= (bytes-to-str converted-message-payload) (bytes-to-str expected-message-payload)))))
    (testing "should return a ziggurat.mapper/->MessagePayload if serialized using nippy"
      (let [expected-message-payload         (assoc (mpr/->MessagePayload proto-message topic-entity) :retry-count 4)
            nippy-serialized-message-payload (nippy/freeze expected-message-payload)
            converted-message-payload        (consumer/convert-and-ack-message nil {:delivery-tag 1} nippy-serialized-message-payload false "default")]
        (is (= (bytes-to-str converted-message-payload) (bytes-to-str expected-message-payload)))))
    (testing "should return nil if message isn't nippy or proto serialized"
      (with-redefs [lb/publish (fn [_ _ _ _] nil)]
        (let [random-bytes-as-message-payload     (.getBytes (String. "Hello World"))
            converted-message-payload           (consumer/convert-and-ack-message nil {:delivery-tag 1} random-bytes-as-message-payload false "default")]
        (is (= converted-message-payload nil)))))))
