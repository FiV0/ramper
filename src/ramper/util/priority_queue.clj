(ns ramper.util.priority-queue
  "A thin wrapper around `clojure.data.priority-map` to simulate a priority queue."
  (:require [clojure.data.priority-map :as priority-map]))

(defrecord PriorityQueue [queue keyfn]
  clojure.lang.ISeq
  (cons [this o]
    (update this :queue assoc o ((:keyfn this) o)))

  (count [this]
    (-> this :queue count))

  (empty [this] (->PriorityQueue (priority-map/priority-map) (:keyfn this)))

  (equiv [this o]
    (and (= (:queue this) (:queue o))
         (= (:keyfn this) (:keyfn o))))

  (first [this]
    (-> this :queue first first))

  (seq [this]
    (->> this :queue seq (map first)))

  (next [this]
    (next (seq this)))

  (more [this]
    (rest (seq this))) ;; more seems to be the Java side of rest

  clojure.lang.IPersistentStack
  (peek [this]
    (first this))

  (pop [this]
    (update this :queue (fn [q] (dissoc q (first (first q))))))

  clojure.lang.Reversible
  (rseq [this]
    (-> this :queue rseq))

  clojure.lang.Sorted
  (comparator [this]
    (let [keyfn (:keyfn this)]
      (comparator (fn [x y] (< (keyfn x) (keyfn y))))))

  (entryKey [this o] ((:keyfn this) o))

  (seqFrom [this k ascending]
    (let [q (:queue this)
          s (if ascending (subseq q >= k) (rsubseq q <= k))]
      (map first s)))

  (seq [this ascending]
    (if ascending (seq this) (rseq this))))

(defn priority-queue
  ([keyfn] (priority-queue keyfn []))
  ([keyfn data]
   (->PriorityQueue (into (priority-map/priority-map) (map (fn [value] [(apply keyfn value) value]) data)) keyfn)))
