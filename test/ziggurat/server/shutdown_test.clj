(ns ziggurat.server.shutdown-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.server :refer [server]]
            [ziggurat.server.test-utils :as tu])
  (:import (org.eclipse.jetty.server Server)
           (org.eclipse.jetty.server.handler StatisticsHandler)))

(deftest http-server-graceful-shutdown-test
  (testing "server should process existing requests within 30000ms when it is stopped"
    (with-redefs [ziggurat.server.routes/handler (fn [_] (fn [_] (Thread/sleep 3000) "pong"))]
      (fix/mount-config)
      (mount/start [#'server])
      (let [http-fut (future (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false))]
        ;; 1000 is the minimum sleep required for the future to run
        (Thread/sleep 1000)
        (mount/stop [#'server])
        @http-fut)))
  (testing "server should discard new requests after the server is stopped"
    (with-redefs [ziggurat.server.routes/handler (fn [_] (fn [_] (Thread/sleep 3000) "pong"))]
      (fix/mount-config)
      (mount/start [#'server])
      (let [http-fut (future (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false))]
        ;; 1000 is the minimum sleep required for the future to run
        (Thread/sleep 1000)
        (mount/stop [#'server])
        (is (thrown? Exception (tu/get (-> (ziggurat-config) :http-server :port) "/ping" false false)))
        @http-fut)))
  (testing "server should stop after the graceful shutdown timeout and discard requests in progress"
    (with-redefs [ziggurat.server.routes/handler  (fn [_] (fn [_] (Thread/sleep 3000) "pong"))
                  ziggurat.server/configure-jetty (fn [^Server server]
                                                    (let [stats-handler   (StatisticsHandler.)
                                                          default-handler (.getHandler server)]
                                                      (.setHandler stats-handler default-handler)
                                                      (.setHandler server stats-handler)
                                                      (.setStopTimeout server 2000)
                                                      (.setStopAtShutdown server true)))]
      (fix/mount-config)
      (mount/start [#'server])
      (let [http-fut (future (tu/get (-> (ziggurat-config) :http-server :port) "/ping" true false))]
        ;; 1000 is the minimum sleep required for the future to run
        (Thread/sleep 1000)
        (mount/stop [#'server])
        (is (thrown? Exception @http-fut))))))
