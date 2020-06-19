(ns ziggurat.server.middleware
  (:require [clojure.string :as str]
            [camel-snake-kebab.core :as csk]
            [cheshire.core :as json]
            [clj-stacktrace.repl :as st]
            [ring.util.response :as ring-resp]
            [sentry-clj.async :as sentry]
            [ziggurat.sentry :refer [sentry-reporter]]
            [ziggurat.util.map :as umap]
            [ziggurat.metrics :as metrics]
            [ring.swagger.swagger-ui :as rsui]
            [ziggurat.config :refer [get-in-config]]))

(defn wrap-default-content-type-json [handler]
  (fn [request]
    (let [response (handler request)
          content-type (ring-resp/get-header response "content-type")]
      (if (or (nil? content-type) (str/starts-with? content-type "application/octet-stream"))
        (ring-resp/content-type response "application/json; charset=utf-8")
        response))))

(defn wrap-hyphenate [handler & args]
  (fn [request]
    (let [response
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

(defn wrap-with-metrics [handler]
  (fn [request]
    (let [response          (handler request)
          request-uri       (:uri request)
          response-status   (:status response)]
      (metrics/increment-count ["http-server" "requests-served"] "count" {:request-uri request-uri :response-status (str response-status)})
      response)))

(defn- swagger-enabled? []
  (true? (get-in-config [:http-server :middlewares :swagger :enabled])))

(defn wrap-swagger [handler]
  (if (swagger-enabled?)
    (rsui/wrap-swagger-ui handler {:path "/swagger-ui"})
    handler))
