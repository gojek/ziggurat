(ns ziggurat.retry
  (:require [ziggurat.sentry :refer [sentry-reporter]]
            [sentry.core :as sentry]))

(defn with-retry* [retry-count wait fn-to-retry]
  (let [res (try
              (fn-to-retry)
              (catch Exception e
                (sentry/report-error sentry-reporter e "Pushing message to rabbitmq failed")
                (if-not (zero? retry-count)
                  ::try-again
                  (throw e))))]
    (if (= res ::try-again)
      (do
        (Thread/sleep (or wait 10))
        (recur (dec retry-count) wait fn-to-retry))
      res)))

(defmacro with-retry [{:keys [count wait]} & body]
  `(with-retry* ~count ~wait (fn [] ~@body)))
