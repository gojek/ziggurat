(ns ziggurat.messaging.rabbitmq.producer-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.rabbitmq.producer :as rm-prod]
            [langohr.channel :as lch]
            [ziggurat.fixtures :as fix]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy]
            [langohr.queue :as lq]
            [langohr.exchange :as le])
  (:import (com.rabbitmq.client Channel Connection)
           (org.apache.kafka.common.header Header)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

(defn- create-mock-channel [] (reify Channel
                                (close [_] nil)))

(deftest producer-test
  (testing "it should publish a message without expiry"
    (let [publish-called? (atom false)
          nippy-called?   (atom false)
          serialized-message (byte-array 1234)
          exchange-name      "exchange-test"
          record-headers-map {"foo-1" (String. serialized-message) "foo-2" (String. serialized-message)}
          props-for-publish  {:content-type "application/octet-stream"
                              :persistent   true
                              :headers      record-headers-map}
          headers            (map #(reify
                                     Header
                                     (key [_] (str "foo-" %))
                                     (value [_] serialized-message)) (range 1 3))
          message-payload    {:foo "bar" :headers headers}
          message-payload-without-headers (dissoc message-payload :headers)
          serialized-message (byte-array 1234)
          expiration         nil]
      (with-redefs [lch/open     (fn [^Connection _] (reify Channel
                                                       (close [_] nil)))
                    lb/publish   (fn [^Channel _ ^String _ ^String _ payload
                                      props]
                                   (when (and (= payload serialized-message)
                                              (= props props-for-publish))
                                     (reset! publish-called? true)))
                    nippy/freeze (fn [payload]
                                   (when (= message-payload-without-headers payload)
                                     (reset! nippy-called? true))
                                   serialized-message)]
        (rm-prod/publish nil exchange-name message-payload expiration))
      (is (true? @publish-called?))
      (is (true? @nippy-called?))))

  (testing "it should publish a message with expiry"
    (let [publish-called? (atom false)
          nippy-called?   (atom false)
          serialized-message (byte-array 1234)
          exchange-name      "exchange-test"
          record-headers-map {"foo-1" (String. serialized-message) "foo-2" (String. serialized-message)}
          props-for-publish  {:content-type "application/octet-stream"
                              :persistent   true
                              :headers      record-headers-map
                              :expiration "10"}
          headers            (map #(reify
                                     Header
                                     (key [_] (str "foo-" %))
                                     (value [_] serialized-message)) (range 1 3))
          message-payload    {:foo "bar" :headers headers}
          message-payload-without-headers (dissoc message-payload :headers)
          serialized-message (byte-array 1234)
          expiration         10]
      (with-redefs [lch/open     (fn [^Connection _] (reify Channel
                                                       (close [_] nil)))
                    lb/publish   (fn [^Channel _ ^String _ ^String _ payload props]
                                   (when (and (= payload serialized-message)
                                              (= props props-for-publish))
                                     (reset! publish-called? true)))
                    nippy/freeze (fn [payload]
                                   (when (= message-payload-without-headers payload)
                                     (reset! nippy-called? true))
                                   serialized-message)]
        (rm-prod/publish nil exchange-name message-payload expiration))
      (is (true? @publish-called?))
      (is (true? @nippy-called?))))

  (testing "when lb/publish function raises an exception, the exception is caught"
    (let [nippy-called?                   (atom false)
          serialized-message              (byte-array 1234)
          exchange-name                   "exchange-test"
          headers                         (map #(reify
                                                  Header
                                                  (key [_] (str "foo-" %))
                                                  (value [_] serialized-message)) (range 1 3))
          message-payload                 {:foo "bar" :headers headers}
          message-payload-without-headers (dissoc message-payload :headers)
          serialized-message              (byte-array 1234)
          expiration                      nil]
      (with-redefs [lch/open     (fn [^Connection _] (reify Channel
                                                       (close [_] nil)))
                    lb/publish   (fn [^Channel _ ^String _ ^String _ _ _]
                                   (throw (Exception. "publish error")))
                    nippy/freeze (fn [payload]
                                   (when (= message-payload-without-headers payload)
                                     (reset! nippy-called? true))
                                   serialized-message)]
        (is (thrown? Exception (rm-prod/publish nil exchange-name message-payload expiration))))
      (is (true? @nippy-called?)))))

(deftest create-and-bind-queue-test
  (testing "it should create a queue,exchange and bind the queue to the exchange but not tag the queue with a dead-letter exchange"
    (let [default-props {:durable true :auto-delete false}
          default-props-with-arguments (assoc default-props :arguments {})
          queue-name "test-queue"
          exchange-name "test-exchange"
          exchange-declare-called? (atom false)
          queue-declare-called? (atom false)
          bind-called? (atom false)]
      (with-redefs [lch/open (fn [^Connection _] (create-mock-channel))
                    lq/declare (fn [^Channel _ ^String queue props]
                                 (when (and (= props default-props-with-arguments)
                                            (= queue-name queue))
                                   (reset! queue-declare-called? true)))
                    le/declare (fn [^Channel _ ^String name ^String type props]
                                 (when (and (= name exchange-name)
                                            (= props default-props))
                                   (reset! exchange-declare-called? true)))
                    lq/bind (fn [^Channel _ ^String queue ^String exchange]
                              (when (and (= queue queue-name)
                                         (= exchange exchange-name))
                                (reset! bind-called? true)))]
        (rm-prod/create-and-bind-queue nil queue-name exchange-name nil))
      (is (true? @bind-called?))
      (is (true? @exchange-declare-called?))
      (is (true? @queue-declare-called?)))))

