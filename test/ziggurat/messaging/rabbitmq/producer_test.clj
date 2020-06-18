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

(use-fixtures :once (join-fixtures [fix/init-rabbit-mq]))

(deftest producer-test
  (testing "it should publish a message without expiry"
    (let [is-publish-called? (atom false)
          header-count 2
          is-nippy-called? (atom false)
          exchange-name "exchange-test"
          record-headers-map {"foo-1" "bar-1" "foo-2" "bar-2"}
          props-for-publish {:content-type "application/octet-stream"
                             :persistent   true
                             :headers      record-headers-map}
          headers (map #(reify
                          Header
                          (key [_] (str "foo-" %))
                          (value [_] (str "bar-" %))) (range 1 3))
          message-payload {:foo "bar" :headers headers}
          expiration nil]
      (with-redefs [lch/open (fn [^Connection _] (reify Channel
                                                   (close [_] nil)))
                    lb/publish (fn [^Channel _ ^String _ ^String _ payload
                                    {:keys [^Boolean mandatory ^String content-type ^String _ ^Map headers
                                            ^Boolean persistent ^Integer _ ^String _ ^String _ ^String expiration ^String _
                                            ^Date _ ^String _ ^String _ ^String _ ^String _]
                                     :or   {mandatory false}}]
                                 (prn "################# mock called")
                                 (when (and (= payload message-payload)
                                            (= headers (:headers props-for-publish)))
                                   (reset! is-publish-called? true)))
                    nippy/freeze (fn [payload]
                                   ((when (= message-payload payload)
                                      (reset! is-nippy-called? true))))]
        (rm-prod/publish nil exchange-name message-payload expiration)))))

