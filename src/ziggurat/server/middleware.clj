(ns ziggurat.server.middleware
  (:require [sentry.core :as sentry]
            [medley.core :as m]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.server.map :as umap]
            [camel-snake-kebab.core :as csk]
            [cheshire.core :as json]
            [clj-stacktrace.repl :as st]
            [clojure.string :as str]
            [ring.util.response :as ring-resp]
            [clojure.walk :as w]))

(defn wrap-default-content-type-json [handler]
  (fn [request]
    (let [response (handler request)
          content-type (ring-resp/get-header response "content-type")]
      (if (or (nil? content-type) (str/starts-with? content-type "application/octet-stream"))
        (ring-resp/content-type response "application/json; charset=utf-8")
        response))))

(defn wrap-hyphenate [handler & args]
  (fn [request]
    (let [{:keys [skip-hyphenation] :as response}
          (handler (update request
                           :params
                           #(umap/nested-map-keys (fn [k] (apply csk/->kebab-case-keyword k args)) %)))]
      response)))

(defn wrap-errors [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (sentry/report-error sentry-reporter ex "Uncaught error in server")
        {:status 500 :body (json/encode {:Error (st/pst-str ex)})}))))
