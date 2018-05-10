(ns ziggurat.map
  (:require [clojure.string :as cstr]))

(defn deep-merge [a b]
  (merge-with (fn [x y]
                (cond (map? y) (deep-merge x y)
                      (nil? x) y
                      :else x))
              a b))

(defn- apply-if-string-and-not-nil
  [parse-fn]
  (fn [v] (if (and (string? v) (some? v)) (parse-fn v) v)))


(def default-value-processors {:bool    (apply-if-string-and-not-nil #(Boolean/parseBoolean %))
                               :int     (apply-if-string-and-not-nil #(Integer/parseInt %))
                               :long    (apply-if-string-and-not-nil #(Long/parseLong %))
                               :bigint  (apply-if-string-and-not-nil bigint)
                               :bigdec  (apply-if-string-and-not-nil bigdec)
                               :float   (apply-if-string-and-not-nil #(Float/parseFloat %))
                               :double  (apply-if-string-and-not-nil #(Double/parseDouble %))
                               :keyword (apply-if-string-and-not-nil keyword)
                               :read    (apply-if-string-and-not-nil read-string)})

(defn- get-new-or-default [conf-key conf-val default-conf-val]
  (let [sanitized-key (-> conf-key
                          (cstr/upper-case)
                          (cstr/replace #"-" "_")
                          (cstr/replace #"/" "_"))
        conf-val (get conf-val sanitized-key)]
    (if (some? conf-val)
      (if (string? default-conf-val)
        conf-val
        ((get default-value-processors (second default-conf-val)) conf-val)))))

(defn- get-key
  [prefix key]
  (if (nil? prefix)
    key
    (str prefix "/" key)))

(defn- flatten-map-kvs
  ([default-map new-map] (flatten-map-kvs default-map new-map nil))
  ([default-map new-map prefix]
   (reduce
     (fn [flatten-map-list [k v]]
       (let [prefix-key (get-key prefix (name k))]
         (if (map? v)
           (concat flatten-map-list (flatten-map-kvs v new-map prefix-key))
           (conj flatten-map-list [prefix-key (get-new-or-default prefix-key new-map v)]))))
     [] default-map)))

(defn flatten-map-and-replace-defaults
  [default-conf new-conf]
  (let [c (into {} (flatten-map-kvs default-conf new-conf))]
    (reduce (fn [new-map [k v]]
              (assoc-in new-map
                        (map keyword
                             (clojure.string/split k #"/"))
                        v))
            {}
            c)))
