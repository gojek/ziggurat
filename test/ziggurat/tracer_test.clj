(ns ziggurat.tracer-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.tracer :as tracer]))

(deftest mount-tracer-test
  (testing "should start JaegerTracer when tracer is enabled"
    (fix/mount-config)
    (mount/start (mount/only [#'tracer/tracer]))
    (is (= "JaegerTracer" (.getSimpleName (.getClass tracer/tracer))))
    (mount/stop))

  (testing "should start NoopTracer when tracer is not enabled"
    (fix/mount-config)
    (with-redefs [ziggurat-config (fn [] {})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop)))
