(ns ziggurat.config-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [clonfig.core :as clonfig]
            [mount.core :as mount]
            [ziggurat.config :refer [-get
                                     -getIn
                                     build-properties
                                     build-consumer-config-properties
                                     build-streams-config-properties
                                     channel-retry-config
                                     config config-file
                                     config-from-env
                                     consumer-config-mapping-table
                                     producer-config-mapping-table
                                     streams-config-mapping-table
                                     default-config get-in-config
                                     rabbitmq-config
                                     set-property
                                     statsd-config
                                     ziggurat-config
                                     ssl-config
                                     create-jaas-properties]]
            [ziggurat.fixtures :as f])
  (:import (java.util ArrayList Properties)))

(deftest config-from-env-test
  (testing "calls clonfig"
    (let [config-values-from-env {:key "val"}]
      (with-redefs [clonfig/read-config (fn [_] config-values-from-env)]
        (is (= config-values-from-env (config-from-env f/test-config-file-name)))))))

(deftest config-test
  (testing "returns merged config from env variables and default values with env variables taking higher precedence"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (-> (config-from-env config-filename)
                                     (update-in [:ziggurat] dissoc :nrepl-server))]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (-> default-config :ziggurat :nrepl-server :port) (-> config :ziggurat :nrepl-server :port))) ;; when env variable is missing, takes default value
        (is (= (-> config-values-from-env :ziggurat :stream-router :default :bootstrap-servers) (-> config :ziggurat :stream-router :default :bootstrap-servers))) ;; when key is not present in default config, takes env variable
        (is (= (-> config-values-from-env :ziggurat :rabbit-mq :delay :queue-name) (-> config :ziggurat :rabbit-mq :delay :queue-name))) ;; when key is present in both default and env variables, takes env variable
        (mount/stop))))

  (testing "returns default interpolated rabbitmq config when not present in env variables"
    (let [app-name                    "application_name"
          config-filename             f/test-config-file-name
          config-values-from-env      (-> (config-from-env config-filename)
                                          (update-in [:ziggurat :rabbit-mq] dissoc :delay)
                                          (assoc-in [:ziggurat :app-name] app-name))
          expected-delay-queue-config {:queue-name           "application_name_delay_queue"
                                       :exchange-name        "application_name_delay_exchange"
                                       :dead-letter-exchange "application_name_instant_exchange"
                                       :queue-timeout-ms     5000}]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= expected-delay-queue-config (-> config :ziggurat :rabbit-mq :delay)))
        (mount/stop)))))

(deftest ziggurat-config-test
  (testing "returns ziggurat config"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:ziggurat config-values-from-env) (ziggurat-config)))
        (mount/stop)))))

(deftest rabbitmq-config-test
  (testing "returns rabbitmq config"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:rabbit-mq (:ziggurat config-values-from-env)) (rabbitmq-config)))
        (mount/stop)))))

(deftest statsd-config-test
  (testing "returns statsd config using the :statsd key"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:statsd (:ziggurat config-values-from-env)) (statsd-config)))
        (mount/stop)))))

(deftest get-in-config-test
  (testing "returns config for key passed"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :http-server :port) (get-in-config [:http-server :port])))
        (mount/stop))))
  (testing "returns config for key passed with default"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)
          default                "test"]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= default (get-in-config [:invalid :value] default)))
        (mount/stop)))))

(deftest channel-retry-config-test
  (testing "returns channel retry config"
    (let [config-filename        f/test-config-file-name
          config-values-from-env (config-from-env config-filename)
          topic-entity           :default
          channel                :channel-1]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :stream-router topic-entity :channels channel :retry)
               (channel-retry-config topic-entity channel)))
        (mount/stop)))))

(deftest java-config-get-test
  (testing "It fetches the correct values for a given config"
    (let [mocked-config    {:a "Apple"
                            :m {:b "Bell"
                                :n {:c "Cat"}}}
          config-keys-list (doto (ArrayList.)
                             (.add "m")
                             (.add "b"))]
      (with-redefs [config-from-env (constantly mocked-config)]
        (mount/start #'config)
        (is (= "Bell" (-getIn config-keys-list)))
        (is (= "Apple" (-get "a")))
        (mount/stop))))
  (testing "-get returns a Java.util.HashMap when the requested config is a clojure map"
    (let [mocked-config {:a {:b "abcd"}}]
      (with-redefs [config-from-env (constantly mocked-config)]
        (mount/start #'config)
        (is (instance? java.util.HashMap (-get "a")))
        (is (= (.get (-get "a") "b") "abcd"))
        (mount/stop))))
  (testing "-getin returns a Java.util.HashMap when the requested config is a clojure map"
    (let [mocked-config    {:a {:b "foo"}
                            :c {:d {:e "bar"}}}
          config-keys-list (doto (ArrayList.)
                             (.add "c")
                             (.add "d"))]
      (with-redefs [config-from-env (constantly mocked-config)]
        (mount/start #'config)
        (is (instance? java.util.HashMap (-getIn config-keys-list)))
        (mount/stop)))))

(deftest test-build-properties
  (let [config-mapping-table (merge consumer-config-mapping-table
                                    producer-config-mapping-table
                                    streams-config-mapping-table)
        set-all-property (partial set-property config-mapping-table)
        build-all-config-properties (partial build-properties set-all-property)]
    (testing "all valid kafka configs"
      (let [config-map         {:auto-offset-reset  :latest
                                :replication-factor 2
                                :group-id           "foo"
                                :enable-idempotence true}
            props              (build-all-config-properties config-map)
            auto-offset-reset  (.getProperty props "auto.offset.reset")
            group-id           (.getProperty props "group.id")
            enable-idempotence (.getProperty props "enable.idempotence")
            replication-factor (.getProperty props "replication.factor")]
        (is (= auto-offset-reset "latest"))
        (is (= replication-factor "2"))
        (is (= enable-idempotence "true"))
        (is (= group-id "foo"))))
    (testing "valid kafka consumer configs converts commit-interval-ms to auto-commit-interval-ms"
      (let [config-map              {:commit-interval-ms 5000}
            props                   (build-consumer-config-properties config-map)
            auto-commit-interval-ms (.getProperty props "auto.commit.interval.ms")]
        (is (= auto-commit-interval-ms "5000"))))
    (testing "valid kafka streams configs does not convert commit-interval-ms to auto-commit-interval-ms"
      (let [config-map              {:commit-interval-ms 5000}
            props                   (build-streams-config-properties config-map)
            auto-commit-interval-ms (.getProperty props "auto.commit.interval.ms" "NOT FOUND")
            commit-interval-ms      (.getProperty props "commit.interval.ms")]
        (is (= auto-commit-interval-ms "NOT FOUND"))
        (is (= commit-interval-ms "5000"))))
    (testing "mapping table for backward compatibility"
      (let [config-map             {:auto-offset-reset-config           "latest"
                                    :changelog-topic-replication-factor 2
                                    :commit-interval-ms                 20000
                                    :consumer-group-id                  "foo"
                                    :default-api-timeout-ms-config      3000
                                    :default-key-serde                  "key-serde"
                                    :default-value-serde                "value-serde"
                                    :key-deserializer-class-config      "key-deserializer"
                                    :key-serializer-class               "key-serializer"
                                    :retries-config                     5
                                    :session-timeout-ms-config          4000
                                    :stream-threads-count               4
                                    :value-deserializer-class-config    "value-deserializer"
                                    :value-serializer-class             "value-serializer"}
            props                   (build-all-config-properties config-map)
            auto-offset-reset       (.getProperty props "auto.offset.reset")
            auto-commit-interval-ms (.getProperty props "auto.commit.interval.ms")
            group-id                (.getProperty props "group.id")
            replication-factor      (.getProperty props "replication.factor")
            default-api-timeout-ms  (.getProperty props "default.api.timeout.ms")
            key-deserializer        (.getProperty props "key.deserializer")
            key-serializer          (.getProperty props "key.serializer")
            session-timeout-ms      (.getProperty props "session.timeout.ms")
            num-stream-threads      (.getProperty props "num.stream.threads")
            retries                 (.getProperty props "retries")
            value-deserializer      (.getProperty props "value.deserializer")
            value-serializer        (.getProperty props "value.serializer")]
        (is (= auto-offset-reset "latest"))
        (is (= auto-commit-interval-ms "20000"))
        (is (= replication-factor "2"))
        (is (= default-api-timeout-ms "3000"))
        (is (= key-deserializer "key-deserializer"))
        (is (= key-serializer "key-serializer"))
        (is (= session-timeout-ms "4000"))
        (is (= num-stream-threads "4"))
        (is (= retries "5"))
        (is (= value-deserializer "value-deserializer"))
        (is (= value-serializer "value-serializer"))
        (is (= group-id "foo"))))
    (testing "non kafka config keys should not be in Properties"
      (let [config-map {:consumer-type                 :joins
                        :producer                      {:foo "bar"}
                        :channels                      {:bar "foo"}
                        :oldest-processed-message-in-s 10
                        :origin-topic                  "origin"
                        :input-topics                  [:foo :bar]
                        :join-cfg                      {:foo "foo" :bar "bar"}
                        :thread-count                  7
                        :poll-timeout-ms-config        10000}
            props      (build-all-config-properties config-map)]
        (doall
         (map (fn [[k _]]
                (let [string-key (str/replace (name k) #"-" ".")
                      not-found  "NOT FOUND!"
                      v          (.getProperty props string-key not-found)]
                  (is (= v not-found))))
              config-map))))
    (testing "should set ssl properties for streams if enabled is set to true"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:auto-offset-reset  :latest
                                  :group-id           "foo"}
              props              (build-streams-config-properties streams-config-map)
              auto-offset-reset  (.getProperty props "auto.offset.reset")
              group-id           (.getProperty props "group.id")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")]
          (is (= auto-offset-reset "latest"))
          (is (= group-id "foo"))
          (is (= ssl-ks-location "/some/location"))
          (is (= ssl-ks-password "some-password")))))
    (testing "should set ssl properties for consumer API if enabled is set to true"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:max-poll-records   500
                                  :enable-auto-commit true}
              props              (build-consumer-config-properties streams-config-map)
              max-poll-records   (.getProperty props "max.poll.records")
              enable-auto-comit  (.getProperty props "enable.auto.commit")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")]
          (is (= max-poll-records  "500"))
          (is (= enable-auto-comit "true"))
          (is (= ssl-ks-location "/some/location"))
          (is (= ssl-ks-password "some-password")))))
    (testing "should set ssl properties for producer API if enabled is set to true"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:batch.size   500
                                  :acks         1}
              props              (build-consumer-config-properties streams-config-map)
              batch-size         (.getProperty props "batch.size")
              acks               (.getProperty props "acks")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")]
          (is (= batch-size  "500"))
          (is (= acks "1"))
          (is (= ssl-ks-location "/some/location"))
          (is (= ssl-ks-password "some-password")))))
    (testing "should not set ssl properties for streams if eenabled is set to false"
      (with-redefs [ssl-config (constantly {:enabled false
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:auto-offset-reset  :latest
                                  :group-id           "foo"}
              props              (build-streams-config-properties streams-config-map)
              auto-offset-reset  (.getProperty props "auto.offset.reset")
              group-id           (.getProperty props "group.id")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")]
          (is (= auto-offset-reset "latest"))
          (is (= group-id "foo"))
          (is (nil? ssl-ks-location))
          (is (nil? ssl-ks-password)))))
    (testing "ssl properties from streams config map overrides the ssl properties provided in [:ziggurat :ssl]"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:auto-offset-reset  :latest
                                  :ssl-keystore-location "/some/different/location"
                                  :ssl-keystore-password "different-password"}
              props              (build-streams-config-properties streams-config-map)
              auto-offset-reset  (.getProperty props "auto.offset.reset")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")]
          (is (= auto-offset-reset "latest"))
          (is (= ssl-ks-location "/some/different/location"))
          (is (= ssl-ks-password "different-password")))))
    (testing "ssl properties create jaas template from the map provided in [:ziggurat :ssl :jaas]"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"
                                            :jaas {:username "myuser"
                                                   :password "mypassword"
                                                   :mechanism "SCRAM-SHA-512"}})]
        (let [streams-config-map {:auto-offset-reset  :latest}
              props              (build-streams-config-properties streams-config-map)
              auto-offset-reset  (.getProperty props "auto.offset.reset")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")
              sasl-jaas-config   (.getProperty props "sasl.jaas.config")]
          (is (= auto-offset-reset "latest"))
          (is (= ssl-ks-location  "/some/location"))
          (is (= ssl-ks-password  "some-password"))
          (is (= sasl-jaas-config (create-jaas-properties "myuser" "mypassword" "SCRAM-SHA-512"))))))
    (testing "ssl properties DO NOT create jaas template if no value is provided for key sequence [:ziggurat :ssl :jaas]"
      (with-redefs [ssl-config (constantly {:enabled true
                                            :ssl-keystore-location "/some/location"
                                            :ssl-keystore-password "some-password"})]
        (let [streams-config-map {:auto-offset-reset  :latest}
              props              (build-streams-config-properties streams-config-map)
              auto-offset-reset  (.getProperty props "auto.offset.reset")
              ssl-ks-location    (.getProperty props "ssl.keystore.location")
              ssl-ks-password    (.getProperty props "ssl.keystore.password")
              sasl-jaas-config   (.getProperty props "sasl.jaas.config")]
          (is (= auto-offset-reset "latest"))
          (is (= ssl-ks-location  "/some/location"))
          (is (= ssl-ks-password  "some-password"))
          (is (nil? sasl-jaas-config)))))))

(deftest test-set-property
  (testing "set-property with empty (with spaces) value"
    (let [properties (Properties.)
          key        :consumer-group-id
          value      "     "
          out-p      (set-property consumer-config-mapping-table properties key value)
          not-found  "NOT FOUND!"]
      (is (= (.getProperty out-p "group.id" not-found) not-found)))))
