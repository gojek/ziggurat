(ns ziggurat.producer-test
  (:require [clojure.test :refer :all]
            [flatland.protobuf.core :as proto]
            [ziggurat.streams :refer [start-streams stop-streams]]
            [ziggurat.fixtures :as fix]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.producer :as producer :refer [producer-properties-map send kafka-producers]]
            [ziggurat.config :as config]))

(use-fixtures :once fix/mount-only-config-and-producer)

(defn stream-router-config-without-producer []
  (str "{:stream-router
    {:default
      {:application-id \"test\"\n :bootstrap-servers \"localhost:9092\"\n
      :stream-threads-count [1 :int]\n
      :origin-topic \"topic\"\n
      :proto-class \"flatland.protobuf.test.Example$Photo\"\n
      :channels {:channel-1 {:worker-count [10 :int]\n :retry {:count   [5 :int]\n :enabled [true :bool]}}}}}}"))

(deftest send-data-with-topic-and-value-test
  (let [topic "test-topic"
        data "Hello World!! from Multiple Kafka Producers"]
    (let [future (send :default topic data)]
      (Thread/sleep 1000)
      (is (-> future .get .partition (>= 0))))))

(deftest send-throws-exception-when-no-producers-are-configured
  (with-redefs-fn
    {#'kafka-producers {}}
    #(let [topic "test-topic"
           data "Hello World!! from non-existant Kafka Producers"]
       (is (not-empty (try (send :default topic data)
                           (catch Exception e (ex-data e))))))))


(deftest producer-properties-map-is-empty-if-no-producers-configured
  ; Here ziggurat-config has been substituted with a custom string which
  ; does not have any valid producer configs.
  (with-redefs-fn
    {#'ziggurat-config stream-router-config-without-producer}
    #(is (empty? (producer-properties-map)))))

(deftest producer-properties-map-is-not-empty-if-producers-are-configured
  ; Here the config is read from config.test.edn which contains
  ; valid producer configs.
  (is (seq (producer-properties-map))))


