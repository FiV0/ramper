(ns ramper.util.priority-queue
  "A thin wrapper around `clojure.data.priority-map` to simulate a priority queue."
  (:require [clojure.data.priority-map :as priority-map]))

;; TODO maybe make compatible with printer

(deftype PriorityQueue [queue keyfn]
  Object
  (toString [this] (str (.seq this)))

  clojure.lang.IPersistentStack
  (seq [this]
    (->> queue seq (map first)))

  (cons [this o]
    (PriorityQueue. (assoc queue o (keyfn o)) keyfn))

  (empty [this] (PriorityQueue. (priority-map/priority-map) keyfn))

  (equiv [this o]
    (and (= queue (.queue o))
         (= keyfn (.keyfn o))))

  (peek [this]
    (-> queue first first))

  (pop [this]
    (PriorityQueue. (dissoc queue (first (first queue))) keyfn))

  clojure.lang.Counted
  (count [this]
    (.count queue))

  clojure.lang.Reversible
  (rseq [this]
    (->> queue rseq (map first)))

  clojure.lang.Sorted
  (comparator [this] (.comparator (.queue this)))

  (entryKey [this o] (keyfn o))

  (seqFrom [this k ascending]
    (let [q (.queue this)]
      (->> (if ascending (subseq q >= k) (rsubseq q <= k))
           (map first))))

  (seq [this ascending]
    (let [q (.queue this)]
      (->> (if ascending (seq q) (rseq q))
           (map first)))))

(defn priority-queue
  ([keyfn] (priority-queue keyfn []))
  ([keyfn data]
   (PriorityQueue. (into (priority-map/priority-map) (map (fn [value] [value (keyfn value)]) data)) keyfn)))