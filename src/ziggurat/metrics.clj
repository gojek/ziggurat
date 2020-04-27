(ns ziggurat.metrics
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ziggurat.config :refer [statsd-config ziggurat-config]]
            [ziggurat.util.java-util :as util]
            [mount.core :refer [defstate]]
            [ziggurat.metrics-interface :as metrics-interface]
            [ziggurat.dropwizard-metrics-wrapper :refer [->DropwizardMetrics]])
  (:gen-class
   :name tech.gojek.ziggurat.internal.Metrics
   :methods [^{:static true} [incrementCount [String String] void]
             ^{:static true} [incrementCount [String String java.util.Map] void]
             ^{:static true} [decrementCount [String String] void]
             ^{:static true} [decrementCount [String String java.util.Map] void]
             ^{:static true} [reportTime [String long] void]
             ^{:static true} [reportTime [String long java.util.Map] void]]))

(def metric-impl (atom nil))

(defn- get-metrics-implementor-constructor []
  (if-let [configured-metrics-class-constructor (get-in (ziggurat-config) [:metrics :constructor])]
    (let [configured-constructor-symbol (symbol configured-metrics-class-constructor)
          constructor-namespace         (namespace configured-constructor-symbol)
          _                             (require [(symbol constructor-namespace)])
          metric-constructor            (resolve configured-constructor-symbol)]

      (if (nil? metric-constructor)
        (throw (ex-info "Incorrect metrics_interface implementation constructor configured. Please fix it." {:constructor-configured configured-constructor-symbol}))
        metric-constructor))
    ->DropwizardMetrics))

(defn initialise-metrics-library []
  (let [metrics-impl-constructor (get-metrics-implementor-constructor)]
    (reset! metric-impl (metrics-impl-constructor))))

(defstate statsd-reporter
  :start (do (log/info "Initializing Metrics")
             (initialise-metrics-library)
             (metrics-interface/initialize @metric-impl (statsd-config)))
  :stop (do (log/info "Terminating Metrics")
            (metrics-interface/terminate @metric-impl)))

(defn intercalate-dot
  [names]
  (str/join "." names))

(defn remove-topic-tag-for-old-namespace
  [additional-tags ns]
  (let [topic-name (:topic_name additional-tags)]
    (dissoc additional-tags (when (some #(= % topic-name) ns) :topic_name))))

(defn- get-all-tags
  [additional-tags metric-namespaces]
  (let [{:keys [app-name env]} (ziggurat-config)
        default-tags {:actor (name app-name)
                      :env   (name env)}]
    (merge additional-tags default-tags)))

(defn- get-metric-namespaces
  [metric-namespaces]
  (if (vector? metric-namespaces)
    [(intercalate-dot metric-namespaces)]
    [metric-namespaces (str (:app-name (ziggurat-config)) "." metric-namespaces)]))

(defn- get-v
  [f d v]
  (if (f v) v d))

(def ^:private get-int (partial get-v number? 1))

(def ^:private get-map (partial get-v map? {}))

(defn- inc-or-dec-count
  "This method increments or decrements the `metric` associated with `metric-namespace` by `n` count
   according to the value of `sign`. Here sign can be '+' or '-'. User can pass tags as 'key-value' pairs
   through `additional_tags`.

   Deprecation Notice: As of version 3.1.0, this function publishes the metrics in two formats.
   The first format appends the service name to `metric-namespace` before publishing while the second format publishes
   the metrics without making any changes to `metric-namespace`.
   E.g. if the passed metric-namespace is `throughput` it will be published as `service_name.throughput` and `throughput`.

   In future releases, Ziggurat will stop publishing metrics in the first format i.e. `service_name.namespace`.
   "
  ([sign metric-namespace metric]
   (inc-or-dec-count sign metric-namespace metric 1 {}))
  ([sign metric-namespace metric n-or-additional-tags]
   (inc-or-dec-count sign metric-namespace metric (get-int n-or-additional-tags) (get-map n-or-additional-tags)))
  ([sign metric-namespace metric n additional-tags]
   (inc-or-dec-count sign {:metric-namespace metric-namespace :metric metric :n n :additional-tags additional-tags}))
  ([sign {:keys [metric-namespace metric n additional-tags]}]
   (let [metric-namespaces (get-metric-namespaces metric-namespace)
         tags              (remove-topic-tag-for-old-namespace (get-all-tags (get-map additional-tags) metric-namespaces) metric-namespace)
         signed-int-value  (sign (get-int n))]
     (doseq [metric-ns metric-namespaces]
       (metrics-interface/update-counter @metric-impl metric-ns metric tags signed-int-value)))))

(def increment-count (partial inc-or-dec-count +))

(def decrement-count (partial inc-or-dec-count -))

(defn multi-ns-increment-count [nss metric additional-tags]
  (doseq [ns nss]
    (increment-count ns metric additional-tags)))

(defn report-histogram
  ([metric-namespaces val]
   (report-histogram metric-namespaces val nil))
  ([metric-namespaces val additional-tags]
   (let [intercalated-metric-namespaces (get-metric-namespaces metric-namespaces)
         tags                           (remove-topic-tag-for-old-namespace (get-all-tags additional-tags metric-namespaces) metric-namespaces)
         integer-value                  (get-int val)
         metric                          "all"]
     (doseq [metric-ns intercalated-metric-namespaces]
       (metrics-interface/update-timing @metric-impl metric-ns metric tags integer-value)))))

(defn report-time
  "This function is an alias for `report-histogram`.

   Deprecation Notice: `report-time` will be removed in the future releases of Ziggurat. It has been renamed to
   `report-histogram`."
  [& args]
  (log/warn "Deprecation Notice: This function is deprecated in favour of ziggurat.metrics/report-histogram. Both functions have the same interface, so please use that function. It will be removed in future releases.")
  (apply report-histogram args))

(defn multi-ns-report-histogram [nss time-val additional-tags]
  (doseq [ns nss]
    (report-histogram ns time-val additional-tags)))

(defn multi-ns-report-time
  "This function is an alias for `multi-ns-report-histogram`.

   Deprecation Notice: `multi-ns-report-time` will be removed in the future releases of Ziggurat. It has been renamed to
   `multi-ns-report-histogram`."
  [nss time-val additional-tags]
  (log/warn "Deprecation Notice: This function is deprecated in favour of ziggurat.metrics/multi-ns-report-histogram. Both functions have the same interface, so please use that function. It will be removed in future releases.")
  (multi-ns-report-histogram nss time-val additional-tags))

(defn -incrementCount
  ([metric-namespace metric]
   (increment-count metric-namespace metric))
  ([metric-namespace metric additional-tags]
   (increment-count metric-namespace metric (util/java-map->clojure-map additional-tags))))

(defn -decrementCount
  ([metric-namespace metric]
   (decrement-count metric-namespace metric))
  ([metric-namespace metric additional-tags]
   (decrement-count metric-namespace metric (util/java-map->clojure-map additional-tags))))

(defn -reportTime
  ([metric-namespace time-val]
   (report-histogram metric-namespace time-val))
  ([metric-namespace time-val additional-tags]
   (report-histogram metric-namespace time-val (util/java-map->clojure-map additional-tags))))
