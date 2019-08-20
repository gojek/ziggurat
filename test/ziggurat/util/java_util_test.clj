(ns ziggurat.util.java-util-test
  (:require [clojure.test :refer :all]
            [ziggurat.util.java-util :refer :all]))

(defn- create-java-list []
  (doto (new java.util.ArrayList)
    (.add "123")))

(defn- create-java-list-of-strings []
  (doto (new java.util.ArrayList)
    (.add "a")
    (.add "b")
    (.add "c")))

(defn- create-java-array []
  (into-array String ["Hello" "World" "!!!"]))

(defn- simple-java-hash-map []
  (doto (new java.util.HashMap)
    (.put ":first" "first value")
    (.put ":second" (create-java-list))
    (.put "third" (create-java-array))
    (.put "fourth" "just-another-value")))

(defn- create-fn-which-returns-java-map []
  (fn [] (simple-java-hash-map)))

(defn- create-fn-which-does-not-return-java-map []
  (fn [] "Hello World!!!"))

(defn- complex-java-hash-map []
  (doto (new java.util.HashMap)
    (.put ":keyword" "value")
    (.put "string-key" (simple-java-hash-map))
    (.put "func" (create-fn-which-returns-java-map))))

(defn- ultra-simple-java-hash-map []
  (doto (new java.util.HashMap)
    (.put "first-key" "first value")
    (.put ":second-keyword" "second value")))

(defn- create-complex-java-list []
  (doto (new java.util.ArrayList)
    (.add "ping")
    (.add (create-java-list))
    (.add (ultra-simple-java-hash-map))
    (.add (doto (java.util.ArrayList.)
            (.add (create-java-list))))))

(defn- verify-that-simple-java-map-is-a-clojure-map
  [map]
  (is (map? map))
  (let [inner-val (:first map)
        java-iterable-val (:second map)
        java-array-val (get map "third")
        inner-val-fourth (get map "fourth")]
    (is (= "first value" inner-val))
    (is (vector? java-iterable-val))
    (is (vector? java-array-val))
    (is (= "just-another-value" inner-val-fourth))))

(deftest creates-stream-routes-map-test
  (testing "Should construct a clojure hash-map for stream routes from a Java HashMap"
    (with-redefs [output-transformer-fn (constantly "output-fn-transformer was called")]
      (let [clojure-hash-map (java-map->clojure-map (complex-java-hash-map))
            val (:keyword clojure-hash-map)
            hash-map-val (get clojure-hash-map "string-key")
            func-val (get clojure-hash-map "func")]
        (is (= "value" val))
        (is (= "output-fn-transformer was called" func-val))
        (verify-that-simple-java-map-is-a-clojure-map hash-map-val)))))

(deftest converts-java-list-to-clojure-vector-test
  (testing "Should convert a complex java list to a clojure vector"
    (let [clojure-vector (java-list->clojure-vector (create-complex-java-list))
          first (first clojure-vector)
          second (doall (second clojure-vector))
          third (nth clojure-vector 2)
          fourth (nth clojure-vector 3)]
      (is (= "ping" first))
      (is (and (vector? second) (some #(= % "123") second)))
      (is (and (map? third) (= "first value" (get third "first-key"))))
      (is (and (vector? fourth) (vector? (nth fourth 0)) (some #(= % "123") (nth fourth 0)))))))

(deftest creates-a-list-of-keywords-from-java-list
  (testing "Should create a list of Clojure keywords from a Java list of strings"
    (let [java-list (list-of-keywords (create-java-list-of-strings))
          expected-list '(:a :b :c)]
      (is (= expected-list java-list)))))

(deftest clojure->java-map-test
  (testing "It returns a java HashMap when a nested clojure map is passed to it"
    (let [clojure-map {:a  1
                       :b  {:c {:d 123}}
                       :c  '(1 2 3)
                       123 "abcd"}
          java-map (clojure->java-map clojure-map)]
      (is (instance? java.util.HashMap java-map))
      (is (instance? java.util.HashMap (.get (.get java-map "b") "c")))
      (is (instance? java.lang.String (.get java-map 123)))
      (is (instance? clojure.lang.PersistentList (.get java-map "c"))))))

(deftest output-transformer-fn-test
  (testing "should return a function which wraps around the original
            and returns a clojure hash map instead of Java map."
    (let [result (apply (output-transformer-fn (create-fn-which-returns-java-map)) [])]
      (verify-that-simple-java-map-is-a-clojure-map result))))

(deftest output-transformer-fn-test-returns-result-as-it-is
  (testing "should return the result of the original function as it is"
    (let [result (apply (output-transformer-fn (create-fn-which-does-not-return-java-map)) [])]
      (is (= "Hello World!!!" result)))))




