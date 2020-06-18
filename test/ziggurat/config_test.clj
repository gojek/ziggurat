(ns ziggurat.config-test
  (:require [clojure.test :refer [deftest is testing]]
            [clonfig.core :as clonfig]
            [mount.core :as mount]
            [ziggurat.config :refer [-get
                                     -getIn
                                     channel-retry-config
                                     config config-file
                                     config-from-env
                                     default-config get-in-config
                                     rabbitmq-config
                                     statsd-config
                                     ziggurat-config]])
  (:import (java.util ArrayList)))

(deftest config-from-env-test
  (testing "calls clonfig"
    (let [config-values-from-env {:key "val"}]
      (with-redefs [clonfig/read-config (fn [_] config-values-from-env)]
        (is (= config-values-from-env (config-from-env "config.test.edn")))))))

(deftest config-test
  (testing "returns merged config from env variables and default values with env variables taking higher precedence"
    (let [config-filename        "config.test.edn"
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
          config-filename             "config.test.edn"
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
    (let [config-filename        "config.test.edn"
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:ziggurat config-values-from-env) (ziggurat-config)))
        (mount/stop)))))

(deftest rabbitmq-config-test
  (testing "returns rabbitmq config"
    (let [config-filename        "config.test.edn"
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:rabbit-mq (:ziggurat config-values-from-env)) (rabbitmq-config)))
        (mount/stop)))))

(deftest statsd-config-test
  (testing "returns statsd config using the :statsd key or :datadog key"
    (let [config-filename        "config.test.edn" ;; inside config.test.edn, both :datadog and :statsd keys are present
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:statsd (:ziggurat config-values-from-env)) (statsd-config)))
        (mount/stop))))
  (testing "returns statsd config using the :statsd key"
    (let [config-filename        "config.test.statsd.only.edn"
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:statsd (:ziggurat config-values-from-env)) (statsd-config)))
        (mount/stop))))
  (testing "returns statsd config using the :datadog key" ;; TODO: remove this test in the future since :datadog key will not be used
    (let [config-filename        "config.test.datadog.only.edn"
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (:datadog (:ziggurat config-values-from-env)) (statsd-config)))
        (mount/stop)))))

(deftest get-in-config-test
  (testing "returns config for key passed"
    (let [config-filename        "config.test.edn"
          config-values-from-env (config-from-env config-filename)]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :http-server :port) (get-in-config [:http-server :port])))
        (mount/stop))))
  (testing "returns config for key passed with default"
    (let [config-filename        "config.test.edn"
          config-values-from-env (config-from-env config-filename)
          default                "test"]
      (with-redefs [config-from-env (fn [_] config-values-from-env)
                    config-file     config-filename]
        (mount/start #'config)
        (is (= default (get-in-config [:invalid :value] default)))
        (mount/stop)))))

(deftest channel-retry-config-test
  (testing "returns channel retry config"
    (let [config-filename        "config.test.edn"
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
