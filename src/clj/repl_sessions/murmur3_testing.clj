(ns repl-sessions.murmur3-testing
  (:require [ramper.util :as util])
  (:import (org.apache.commons.codec.digest MurmurHash3)
           (it.unimi.dsi.bits BitVector)
           (it.unimi.dsi.sux4j.mph Hashes)
           (com.google.common.hash Hashing)
           (com.google.common.primitives Longs)
           (java.util Arrays)))

(defrecord MurmurHash [first second])

(defn bytes->murmur-hash-commens [bytes]
  (let [hash-array (MurmurHash3/hash128 bytes)]
    #_(->MurmurHash (first hash-array) (second hash-array))
    (vector (first hash-array) (second hash-array))))

(-> (util/rand-str 20) util/string->bytes bytes->murmur-hash-commens)

(def guava-murmur3-128 (Hashing/murmur3_128))

(defn bytes->murmur-hash-guava [bytes]
  (let [hash-code (.hashBytes guava-murmur3-128 bytes)
        ba1 (.asBytes hash-code)
        ;; ba2 (Arrays/copyOfRange ba1 8 16)
        ]
    #_(->MurmurHash (first hash-array) (second hash-array))
    #_(vector (Longs/fromByteArray ba1) (Longs/fromByteArray ba2))
    ba1))

(-> (util/rand-str 20) util/string->bytes bytes->murmur-hash-guava )

(defn testing-hash-fn [hash-fn iterations]
  (dotimes [_ iterations]
    (-> (util/rand-str 20) util/string->bytes hash-fn)))

(time
 (testing-hash-fn bytes->murmur-hash-commens 1000000))

(time
 (testing-hash-fn bytes->murmur-hash-guava 1000000))

;; the commons MurmurHash3 also accepts strings
(defn testing-hash-fn2 [hash-fn iterations]
  (dotimes [_ iterations]
    (-> (util/rand-str 20) hash-fn)))

(time (testing-hash-fn2 bytes->murmur-hash-commens 1000000))
