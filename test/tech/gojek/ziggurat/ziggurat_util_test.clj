(ns tech.gojek.ziggurat.ziggurat-util-test
  (:require [clojure.test :refer :all]
            [ziggurat.java.utils.ziggurat-util :refer :all]))

(defn- create-java-list []
  (doto (new java.util.ArrayList)
    (.add "123")))

(defn- create-java-array []
  (into-array String ["Hello" "World" "!!!"]))

(defn simple-java-hash-map []
  (doto (new java.util.HashMap)
    (.put ":first" "first value")
    (.put ":second" (create-java-list))
    (.put "third"  (create-java-array))
    (.put "fourth" "just-another-value")))

(defn java-hash-map-with-a-hash-map []
  (doto (new java.util.HashMap)
    (.put ":keyword" "value")
    (.put "string-key" (simple-java-hash-map))))

(deftest creates-stream-routes-map-test
  (testing "Should construct a clojure hash-map for stream routes from a Java HashMap"
    (let [clojure-hash-map (create-clojure-hash-map (java-hash-map-with-a-hash-map))
          val (:keyword clojure-hash-map)
          val-hash-map (get clojure-hash-map "string-key")]
      (is (= "value" val))
      (is (map? val-hash-map))
      (let [inner-val (:first val-hash-map)
            java-iterable-val (:second val-hash-map)
            java-array-val (get val-hash-map "third")
            inner-val-fourth (get val-hash-map "fourth")]
        (is (= "first value" inner-val))
        (is (seq? java-iterable-val))
        (is (seq? java-array-val))
        (is (= "just-another-value" inner-val-fourth))))))

(defn ultra-simple-java-hash-map []
  (doto (new java.util.HashMap)
    (.put "first-key" "first value")
    (.put ":second-keyword" "second value")))

(defn create-complex-java-list []
  (doto (new java.util.ArrayList)
    (.add "ping")
    (.add (create-java-list))
    (.add (ultra-simple-java-hash-map))))

(deftest converts-java-list-to-clojure-vector-test
  (testing "Should convert a complex java list to a clojure vector"
    (let [clojure-vector (doall (create-clojure-vector (create-complex-java-list)))
          first (first clojure-vector)
          second (doall (second clojure-vector))
          third (nth clojure-vector 2)]
      (is (= "ping" first))
      (is (and (seq? second) (some #(= % "123") second)))
      (is (and (map? third) (= "first value" (get third "first-key")))))))




