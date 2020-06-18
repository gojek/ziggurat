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
            [langohr.exchange :as le]
            [langohr.basic :as lb]
            [taoensso.nippy :as nippy])
  (:import (java.util Date Map)
           (com.rabbitmq.client Channel Connection)
           (org.apache.kafka.common.header.internals RecordHeaders)
           (org.apache.kafka.common.header Header)))

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq
                                    fix/silence-logging]))

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

  (testing "when lb/publish function raises an exception, the exception is caught"
    (let [publish-called?                 (atom false)
          nippy-called?                   (atom false)
          serialized-message              (byte-array 1234)
          exchange-name                   "exchange-test"
          record-headers-map              {"foo-1" (String. serialized-message) "foo-2" (String. serialized-message)}
          props-for-publish               {:content-type "application/octet-stream"
                                           :persistent   true
                                           :headers      record-headers-map}
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
                    lb/publish   (fn [^Channel _ ^String _ ^String _ payload
                                      props]
                                   (throw (Exception. "publish error")))
                    nippy/freeze (fn [payload]
                                   (when (= message-payload-without-headers payload)
                                     (reset! nippy-called? true))
                                   serialized-message)]
        (is (thrown? Exception (rm-prod/publish nil exchange-name message-payload expiration))))
      (is (true? @nippy-called?)))))

