(ns ziggurat.messaging.util-test
  (:require [clojure.test :refer :all]
            [ziggurat.messaging.util :refer :all])
  (:import (com.rabbitmq.client DnsRecordIpAddressResolver ListAddressResolver)))

(deftest test-get-name-with-prefix-topic
  (testing "given a topic-entity and queue-name, it returns a snake case string with topic-entity as the prefix of queue-name"
    (let [topic-entity    :topic-name
          queue-name      "queue_name"
          expected-string "topic-name_queue_name"]
      (is (= (prefixed-queue-name topic-entity queue-name) expected-string)))))

(deftest create-address-resolver-test
  (testing "creates an instance of DnsRecordIpAddressResolver if the value of :address-resolver config is :dns"
    (let [address-resolver (create-address-resolver {:hosts "some.dns.com"
                                                     :port  15672
                                                     :address-resolver :dns})]
      (is (true? (instance? DnsRecordIpAddressResolver address-resolver)))))
  (testing "creates an instance of DnsRecordIpAddressResolver if the value of :address-resolver config is NOT provided"
    (let [address-resolver (create-address-resolver {:hosts "some.dns.com"
                                                     :port  15672})]
      (is (true? (instance? DnsRecordIpAddressResolver address-resolver)))))
  (testing "creates an instance of DnsRecordIpAddressResolver if the value of :address-resolver config is :ip-list"
    (let [address-resolver (create-address-resolver {:hosts "some.dns.com"
                                                     :port  15672
                                                     :address-resolver :ip-list})]
      (is (true? (instance? ListAddressResolver address-resolver))))))
