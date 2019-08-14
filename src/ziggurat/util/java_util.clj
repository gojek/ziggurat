(ns ziggurat.util.java-util
  (:require [clojure.string :as str]
            [clojure.walk :refer [stringify-keys postwalk]])
  (:import (java.util Map Arrays)))

(declare java->clojure-map)
(declare create-clojure-vector)
(declare create-clojure-vector-from-array)

(defn- is-java-array?
  [array]
  (str/starts-with? (.getName (.getClass array)) "["))

(defn create-clojure-vector
  [^java.lang.Iterable java-list]
  (let [cloj-seq (seq java-list)]
    (vec (map (fn [x]
                (cond
                  (instance? java.util.Map x) (java->clojure-map x)
                  (instance? java.lang.Iterable x) (create-clojure-vector x)
                  :else x))
              cloj-seq))))

(defn create-clojure-vector-from-array
  [java-array]
  (create-clojure-vector (Arrays/asList java-array)))

(defn get-key
  [key]
  (if (str/starts-with? key ":")
    (keyword (subs key 1))
    key))

(defn list-of-keywords
  [keys]
  (map keyword (seq keys)))

(defn java->clojure-map [^Map java-map]
  "A util method for converting a Java HashMap (Map) to a clojure hash-map.
   This but can be used to convert any Java HashMap where following
   are required:
     1) Some inner value of type `java.util.Map` need to be
        converted to Clojure's hash-map (or `clojure.lang.APersistentMap`).
     2) Some inner value which either is a Java Array or extends
        `java.lang.Iterable` (e.g. ArraLists, HashSets) need to
        be converted to Clojure sequences.

   It supports conversion of Java Maps to Clojure's `APersistent` type;
   and Java Lists and Arrays or anything which extends `java.lang.Iterable`
   to clojure's PersistentVector type."
  (reduce
   (fn [map entry]
     (let [key (.getKey entry)
           value (.getValue entry)]
       (cond (instance? Map value)                (assoc map (get-key key) (java->clojure-map value))
             (instance? java.lang.Iterable value) (assoc map (get-key key) (create-clojure-vector value))
             (is-java-array? value)               (assoc map (get-key key) (create-clojure-vector-from-array value))
             :else                                (assoc map (get-key key) value))))
   (hash-map)
   (.toArray (.entrySet java-map))))

(defn clojure->java-map [clojure-map]
  "A util method to convert nested clojure.lang.PersistentArrayMap (or clojure.lang.PersistentHashMap to java.util.HashMap. It only converts a map and does not affect lists and vectors."
  (let [string-keys-map (stringify-keys clojure-map)]
    (postwalk (fn [value]
                (if (map? value)
                  (java.util.HashMap. value)
                  value))
              string-keys-map)))
