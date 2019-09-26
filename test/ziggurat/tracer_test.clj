(ns ziggurat.tracer-test
  (:require [clojure.test :refer :all]
            [mount.core :as mount]
            [ziggurat.config :refer [ziggurat-config]]
            [ziggurat.fixtures :as fix]
            [ziggurat.tracer :as tracer])
  (:import [io.opentracing.mock MockTracer]))

(use-fixtures :once fix/silence-logging)

(defn tracer-provider []
  (MockTracer.))

(deftest mount-tracer-test
  (testing "should start JaegerTracer when tracer is enabled and tracer provider is empty"
    (fix/mount-config)
    (mount/start (mount/only [#'tracer/tracer]))
    (is (= "JaegerTracer" (.getSimpleName (.getClass tracer/tracer))))
    (mount/stop))

  (testing "should start JaegerTracer when tracer is enabled and tracer provider is nil"
    (fix/mount-config)
    (mount/start (mount/only [#'tracer/tracer]))
    (is (= "JaegerTracer" (.getSimpleName (.getClass tracer/tracer))))
    (mount/stop))

  (testing "should execute create custom tracer when tracer is enabled and tracer provider is set"
    (fix/mount-config)
    (with-redefs [ziggurat-config (fn [] {:tracer {:enabled         true
                                                   :tracer-provider "ziggurat.tracer-test/tracer-provider"}})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "MockTracer" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop))

  (testing "should handle gracefully when custom tracer provider returns nil and create NoopTracer"
    (fix/mount-config)
    (with-redefs [tracer-provider (fn [] nil)
                  ziggurat-config (fn [] {:tracer {:enabled         true
                                                   :tracer-provider "ziggurat.tracer-test/tracer-provider"}})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop))

  (testing "should handle gracefully when custom tracer provider returns non tracer instance and create NoopTracer"
    (fix/mount-config)
    (with-redefs [tracer-provider (fn [] "")
                  ziggurat-config (fn [] {:tracer {:enabled         true
                                                   :tracer-provider "ziggurat.tracer-test/tracer-provider"}})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop))

  (testing "should start NoopTracer when tracer is not enabled"
    (fix/mount-config)
    (with-redefs [ziggurat-config (fn [] {:tracer {:enabled false}})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop))

  (testing "should start NoopTracer when tracer is not configured"
    (fix/mount-config)
    (with-redefs [ziggurat-config (fn [] {})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop))

  (testing "should handle create tracer exception gracefully and create NoopTracer"
    (fix/mount-config)
    (with-redefs [tracer-provider (fn [] (throw (RuntimeException.)))
                  ziggurat-config (fn [] {:tracer {:enabled         true
                                                   :tracer-provider "ziggurat.tracer-test/tracer-provider"}})]
      (mount/start (mount/only [#'tracer/tracer]))
      (is (= "NoopTracerImpl" (.getSimpleName (.getClass tracer/tracer)))))
    (mount/stop)))
