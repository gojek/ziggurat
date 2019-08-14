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

(defn java-list->clojure-vector
  [^java.lang.Iterable java-list]
  (postwalk (fn [element]
              (cond
                (instance? java.util.Map element) (java->clojure-map element)
                (instance? java.lang.Iterable element) (vec (seq element))
                :else element))
            (vec (seq java-list))))

(defn java-array->clojure-vector
  [java-array]
  (java-list->clojure-vector (Arrays/asList java-array)))

(defn get-key
  [key]
  (if (str/starts-with? key ":")
    (keyword (subs key 1))
    key))

(defn list-of-keywords
  [keys]
  (map keyword (seq keys)))

(defn output-transformer-fn
  "This function returns a function which transforms the output of the provided
   function `func`, if the output is an instance of `java.util.Map`."
  [func]
  (fn [& args]
    (let [result (apply func args)]
      (if (instance? Map result)
        (java->clojure-map result)
        result))))

(defn java->clojure-map
  "A util method for converting a Java HashMap (Map) to a clojure hash-map.
   This but can be used to convert any Java HashMap where following
   are required:
     1) An inner value of type `java.util.Map` need to be
        converted to Clojure's hash-map (or `clojure.lang.APersistentMap`).
     2) An inner value which either is a Java Array or extends
        `java.lang.Iterable` (e.g. ArraLists, HashSets) need to
        be converted to Clojure sequences.
     3) An inner value which is a Clojure function but returns a java.util.Map
        as output. This method will wrap the function using `output-transformer-fn`
        to return the output as a clojure map.

   It supports conversion of Java Maps to Clojure's `APersistent` type;
   and Java Lists and Arrays or anything which extends `java.lang.Iterable`
   to clojure's PersistentVector type."
  [^Map java-map]
  (reduce
   (fn [map entry]
     (let [key (.getKey entry)
           value (.getValue entry)]
       (cond (instance? Map value)                (assoc map (get-key key) (java->clojure-map value))
             (instance? java.lang.Iterable value) (assoc map (get-key key) (java-list->clojure-vector value))
             (instance? clojure.lang.IFn value)   (assoc map (get-key key) (output-transformer-fn value))
             (is-java-array? value)               (assoc map (get-key key) (java-array->clojure-vector value))
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
