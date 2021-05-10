(ns ramper.util.lru-immutable-test
  (:require [clojure.core.async :as async]
            [clojure.core.cache.wrapped :as cw]
            [ramper.util.lru :as lru :refer [Cache add check]]
            [ramper.util.lru-immutable :as lru-im]))

(defn random-string [n]
  (str (rand-int n)))

(def nb-threads 48)
(def nb-entries 10M)
(def cache (lru-im/create-lru-cache {} 1000000 lru/string->bytes))

(defn one-cache-loop [n]
  (loop [n n]
    (if (= 0 (rand-int 2))
      (add cache (random-string nb-entries))
      (check cache (random-string nb-entries)))
    (when (< 0 n)
      (recur (dec n)))))

(comment
  (time (one-cache-loop 1000000)))

(def entries-per-thread 100000)

(defn time-taken
  "Returns the execution time of `func` in milliseconds."
  [func]
  (let [ts (System/currentTimeMillis)]
    (func)
    (- (System/currentTimeMillis) ts)))

(defn testing-with-futures []
  (let [func #(future (one-cache-loop entries-per-thread))
        threads (repeatedly nb-threads func)]
    (dorun threads)
    (run! deref threads)))

(def time-with-futures (time-taken testing-with-futures))

;; throughput per second
(/ (* nb-threads entries-per-thread) (/ time-with-futures 1000))


;; (defn testing-with)
