(ns ziggurat.init-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.init :as init]
            [ziggurat.streams :as streams]))

(deftest start-calls-actor-start-fn
  (testing "The actor start fn starts after the lambda internal state and can read config"
    (with-redefs [streams/start-stream (constantly nil)
                  streams/stop-stream  (constantly nil)
                  config/config-file   "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #(deliver retry-count (-> (config/ziggurat-config) :retry :count)) #())
        (init/stop #())
        (is (= 5 (deref retry-count 10000 ::failure)))))))

(deftest stop-calls-actor-stop-fn
  (testing "The actor stop fn is called before stopping the lambda internal state"
    (with-redefs [streams/start-stream (constantly nil)
                  streams/stop-stream  (constantly nil)
                  config/config-file   "config.test.edn"]
      (let [retry-count (promise)]
        (init/start #() #())
        (init/stop #(deliver retry-count (-> (config/ziggurat-config) :retry :count)))
        (is (= 5 (deref retry-count 10000 ::failure)))))))
