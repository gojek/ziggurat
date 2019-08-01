(ns ziggurat.java.utils.ziggurat-util
  (:require [clojure.string :as str])
  (:import (java.util Map Arrays))
  (:gen-class
   :name tech.gojek.ziggurat.ZigguratUtil
   :methods [^{:static true} [createClojureHashMap [java.util.Map] clojure.lang.APersistentMap]]))

(declare create-clojure-hash-map)
(declare create-clojure-vector)
(declare create-clojure-vector-from-array)

(defn- is-java-array?
  [array]
  (str/starts-with? (.getName (.getClass array)) "["))

(defn create-clojure-vector
  [^java.lang.Iterable java-list]
  (let [cloj-seq (seq java-list)]
    (map (fn [x]
           (cond
             (instance? java.util.Map x) (create-clojure-hash-map x)
             (instance? java.lang.Iterable x) (create-clojure-vector x)
             :else x))
         cloj-seq)))

(defn create-clojure-vector-from-array
  [java-array]
  (create-clojure-vector (Arrays/asList java-array)))

(defn get-key
  [key]
  (if (str/starts-with? key ":")
    (keyword (subs key 1))
    key))

(defn create-clojure-hash-map [^Map java-map]
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
       (cond (instance? Map value)                (assoc map (get-key key) (create-clojure-hash-map value))
             (instance? java.lang.Iterable value) (assoc map (get-key key) (create-clojure-vector value))
             (is-java-array? value)               (assoc map (get-key key) (create-clojure-vector-from-array value))
             :else                                (assoc map (get-key key) value))))
   (hash-map)
   (.toArray (.entrySet java-map))))

(defn -createClojureHashMap
  [java-map]
  (create-clojure-hash-map java-map))
