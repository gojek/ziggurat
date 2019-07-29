(ns ziggurat.java.utils.ziggurat-util
  (:require [clojure.string :as str])
  (:import (java.util Map))
  (:gen-class
   :name tech.gojek.ziggurat.ZigguratUtil
   :methods [^{:static true} [createClojureHashMap [java.util.Map] clojure.lang.APersistentMap]]))

(defn -createClojureHashMap [^Map java-map]
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
       (if (instance? Map value)
         (assoc map (keyword key) (-createClojureHashMap value))
         (if (or (instance? java.lang.Iterable value)
                 (str/starts-with? (.getName (.getClass value)) "["))
           (assoc map (keyword key) (seq value))
           (assoc map (keyword key) value)))))
   (hash-map)
   (.toArray (.entrySet java-map))))
