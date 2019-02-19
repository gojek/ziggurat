(ns ziggurat.config-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :refer :all]
            [mount.core :as mount]
            [clonfig.core :as clonfig]))

(deftest config-from-env-test
  (testing "calls clonfig"
    (let [config-values-from-env {:key "val"}]
      (with-redefs [clonfig/read-config (fn [_] config-values-from-env)]
        (is (= config-values-from-env (config-from-env "config.test.edn")))))))

(deftest config-test
  (testing "returns merged config from env variables and default values with env variables taking higher precedence"
    (let [config-values-from-env (-> (config-from-env "config.test.edn")
                                     (update-in [:ziggurat] dissoc :nrepl-server))]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (-> default-config :ziggurat :nrepl-server :port) (-> config :ziggurat :nrepl-server :port))) ;; when env variable is missing, takes default value
        (is (= (-> config-values-from-env :ziggurat :stream-router :default :bootstrap-servers) (-> config :ziggurat :stream-router :default :bootstrap-servers))) ;; when key is not present in default config, takes env variable
        (is (= (-> config-values-from-env :ziggurat :rabbit-mq :delay :queue-name) (-> config :ziggurat :rabbit-mq :delay :queue-name))) ;; when key is present in both default and env variables, takes env variable
        (mount/stop))))

  (testing "returns default interpolated rabbitmq config when not present in env variables"
    (let [app-name "application_name"
          config-values-from-env (-> (config-from-env "config.test.edn")
                                     (update-in [:ziggurat :rabbit-mq] dissoc :delay)
                                     (assoc-in [:ziggurat :app-name] app-name))
          expected-delay-queue-config {:queue-name           "application_name_delay_queue"
                                       :exchange-name        "application_name_delay_exchange"
                                       :dead-letter-exchange "application_name_instant_exchange"
                                       :queue-timeout-ms     5000}]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= expected-delay-queue-config (-> config :ziggurat :rabbit-mq :delay)))
        (mount/stop)))))

(deftest ziggurat-config-test
  (testing "returns ziggurat config"
    (let [config-values-from-env (config-from-env "config.test.edn")]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (:ziggurat config-values-from-env) (ziggurat-config)))
        (mount/stop)))))

(deftest rabbitmq-config-test
  (testing "returns rabbitmq config"
    (let [config-values-from-env (config-from-env "config.test.edn")]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (:rabbit-mq (:ziggurat config-values-from-env)) (rabbitmq-config)))
        (mount/stop)))))

(deftest get-in-config-test
  (testing "returns config for key passed"
    (let [config-values-from-env (config-from-env "config.test.edn")]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :http-server :port) (get-in-config [:http-server :port])))
        (mount/stop)))))

(deftest channel-retry-config-test
  (testing "returns channel retry config"
    (let [config-values-from-env (config-from-env "config.test.edn")
          topic-entity :default
          channel :channel-1]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :stream-router topic-entity :channels channel :retry)
               (channel-retry-config topic-entity channel)))))))

(deftest retry-config-test
  (testing "returns retry config"
    (let [config-values-from-env (config-from-env "config.test.edn")]
      (with-redefs [config-from-env (fn [_] config-values-from-env)]
        (mount/start #'config)
        (is (= (-> config-values-from-env :ziggurat :retry) (retry-config)))))))
