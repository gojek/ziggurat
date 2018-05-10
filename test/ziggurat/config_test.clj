(ns ziggurat.config-test
  (:require [clojure.test :refer :all]
            [ziggurat.config :as config]
            [ziggurat.external.yggdrasil :as yggdrasil]))


;(defn make-config [config-file]
;  (let [edn-conf (edn-config config-file)
;        env-config (config-from-env config-file)
;        {:keys [host port connection-timeout-in-ms]} (:yggdrasil env-config)
;        env (:env env-config)
;        app-name (:app-name env-config)
;        yggdrasil-raw-config (yggdrasil/get-config app-name host port env connection-timeout-in-ms)]
;    (if (nil? yggdrasil-raw-config)
;      env-config
;      (umap/deep-merge (umap/flatten-map-and-replace-defaults edn-conf
;                                                              yggdrasil-raw-config)
;                       env-config))))


(deftest make-config-test
  (testing "It returns config from yggdrasil, when configurations are available from yggdrasil"
    (with-redefs [config/edn-config (constantly {:env     "production"
                                                :port    [9000 :int]
                                                :enabled [true :bool]})
                 config/config-from-env (constantly {:env     "integration"
                                                     :port    8080
                                                     :enabled false})
                 yggdrasil/get-config (constantly {"ENV"     "test"
                                                   "PORT"    "80"
                                                   "ENABLED" "false"})]
     (let [conf (config/make-config "dummy-file")]
       (is (= (:env conf) "test"))
       (is (= (:port conf) 80))
       (is (false? (:enabled conf))))))

  (testing "It returns config from env, when configurations are not available from yggdrasil"
    (with-redefs [config/edn-config (constantly {:env     "production"
                                                 :port    [9000 :int]
                                                 :enabled [true :bool]})
                  config/config-from-env (constantly {:env     "integration"
                                                      :port    8080
                                                      :enabled false})
                  yggdrasil/get-config (constantly nil)]
      (let [conf (config/make-config "dummy-file")]
        (is (= (:env conf) "integration"))
        (is (= (:port conf) 8080))
        (is (false? (:enabled conf)))))))
