(ns ziggurat.retry
  (:require [ziggurat.sentry :refer [sentry-reporter]]
            [sentry.core :as sentry]))

(defn with-retry* [retry-count wait fn-to-retry fn-on-failure]
  (let [res (try
              (fn-to-retry)
              (catch Throwable e
                (fn-on-failure e)
                (if-not (zero? retry-count)
                  ::try-again
                  (throw e))))]
    (if (= res ::try-again)
      (do
        (Thread/sleep (or wait 10))
        (recur (dec retry-count) wait fn-to-retry fn-on-failure))
      res)))

(defmacro with-retry [{:keys [count wait on-failure]} & body]
  `(with-retry* ~count ~wait (fn [] ~@body) ~on-failure))
