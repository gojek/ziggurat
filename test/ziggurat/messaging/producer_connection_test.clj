(ns ziggurat.messaging.producer-connection-test
  (:require [clojure.test :refer :all]
            [ziggurat.fixtures :as fix]
            [mount.core :as mount]
            [ziggurat.config :as config]
            [ziggurat.messaging.producer_connection :as mc :refer [producer-connection]]
            [ziggurat.messaging.connection-helper :as connection-helper :refer [create-connection create-rmq-connection]]
            [ziggurat.util.error :refer [report-error]]))

(use-fixtures :once fix/mount-config-with-tracer)

(deftest connection-test
  (testing "does not create thread-pool for producer connection"
    (let [executor-present? (atom false)
          orig-rmq-connect  create-rmq-connection
          ziggurat-config   (config/ziggurat-config)
          stream-routes     {:default {:handler-fn (constantly :channel-1)
                                       :channel-1  (constantly :success)}}]
      (with-redefs [create-rmq-connection   (fn [cf provided-config]
                                              (when (some? (:executor provided-config))
                                                (reset! executor-present? true))
                                              (orig-rmq-connect cf provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :jobs {:instant {:worker-count 4}}
                                                              :retry {:enabled true}
                                                              :stream-router {:default {:channels {:channel-1 {:worker-count 10}}}}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is (false? @executor-present?))))))

(deftest start-connection-test-with-tracer-disabled
  (testing "if retry is enabled and channels are not present it should create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    create-rmq-connection
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :success)}}]
      (with-redefs [create-rmq-connection   (fn [cf provided-config]
                                              (reset! rmq-connect-called? true)
                                              (orig-rmq-connect cf provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is @rmq-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    create-rmq-connection
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default {:handler-fn (constantly :success)}}]
      (with-redefs [create-rmq-connection   (fn [cf provided-config]
                                              (reset! rmq-connect-called? true)
                                              (orig-rmq-connect cf provided-config))
                    config/ziggurat-config (constantly (-> ziggurat-config
                                                           (assoc :retry {:enabled false})
                                                           (dissoc :tracer)))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is (not @rmq-connect-called?)))))



  (testing "if retry is disabled and channels are present it should create connection"
    (let [rmq-connect-called? (atom false)
          orig-rmq-connect    create-rmq-connection
          ziggurat-config     (config/ziggurat-config)
          stream-routes       {:default   {:handler-fn (constantly :channel-1)
                                           :channel-1  (constantly :success)}
                               :default-1 {:handler-fn (constantly :channel-3)
                                           :channel-3  (constantly :success)}}]
      (with-redefs [create-rmq-connection   (fn [cf provided-config]
                                              (reset! rmq-connect-called? true)
                                              (orig-rmq-connect cf provided-config))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}
                                                              :tracer {:enabled false}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is @rmq-connect-called?))))

  (testing "if retry is enabled and channels are not present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       connection-helper/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default {:handler-fn (constantly :success)}}]
      (with-redefs [connection-helper/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled true}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is @create-connect-called?))))

  (testing "if retry is disabled and channels are not present it should not create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       connection-helper/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default {:handler-fn (constantly :success)}}]
      (with-redefs [connection-helper/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is (not @create-connect-called?)))))

  (testing "if retry is disabled and channels are present it should create connection"
    (let [create-connect-called? (atom false)
          orig-create-conn       connection-helper/create-connection
          ziggurat-config        (config/ziggurat-config)
          stream-routes          {:default   {:handler-fn (constantly :channel-1)
                                              :channel-1  (constantly :success)}
                                  :default-1 {:handler-fn (constantly :channel-3)
                                              :channel-3  (constantly :success)}}]
      (with-redefs [connection-helper/create-connection   (fn [provided-config tracer-enabled]
                                             (reset! create-connect-called? true)
                                             (orig-create-conn provided-config tracer-enabled))
                    config/ziggurat-config (constantly (assoc ziggurat-config
                                                              :retry {:enabled false}))]
        (-> (mount/only #{#'producer-connection})
            (mount/with-args {:stream-routes stream-routes})
            (mount/start))
        (mount/stop #'producer-connection)
        (is @create-connect-called?)))))

(deftest start-connection-test-with-errors
  (testing "if rabbitmq connect throws an error, it gets reported"
    (let [stream-routes     {:default {:handler-fn (constantly :success)}}
          report-fn-called? (atom false)]
      (with-redefs [create-connection (fn [_ _]
                                        (throw (Exception. "Error")))
                    report-error      (fn [_ _] (reset! report-fn-called? true))]
        (try
          (-> (mount/only #{#'producer-connection})
              (mount/with-args {:stream-routes stream-routes})
              (mount/start))
          (catch Exception e
            (mount/stop #'producer-connection)))
        (is @report-fn-called?)))))