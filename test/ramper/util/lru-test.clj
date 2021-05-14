(ns ramper.util.lru-test
  (:require [ramper.util.lru :as lru :refer [Cache add check]]))

(defn random-string [n]
  (str (rand-int n)))

(def nb-threads 48)
(def nb-entries 10M)
(def cache (lru/create-lru-cache 1000000 lru/string->bytes nb-threads))

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

(comment
  (def time-with-futures (time-taken testing-with-futures))

  ;; throughput per second
  (float (/ (* nb-threads entries-per-thread) (/ time-with-futures 1000)))
  ;; => 25219.355

  )

(defn testing-with-threads []
  (let [func #(future (one-cache-loop entries-per-thread))
        threads (repeatedly nb-threads func)]
    (dorun threads)
    (run! deref threads)))

(comment
  (def time-with-threads (time-taken testing-with-threads))

  ;; throughput per second
  (float (/ (* nb-threads entries-per-thread) (/ time-with-threads 1000)))

  )
