(ns ramper.util.priority-queue
  "A thin wrapper around `clojure.data.priority-map` to simulate a priority queue."
  (:require [clojure.data.priority-map :as priority-map]))

(deftype PriorityQueue [queue keyfn]
  Object
  (toString [this] (str (.seq this)))

  ;; Is this a good idea?
  clojure.lang.IPersistentMap
  (assoc [_this o k]
    (assert (= k (keyfn o)))
    (PriorityQueue. (assoc queue o k) keyfn))
  (without [_this o]
    (PriorityQueue. (dissoc queue o) keyfn))

  clojure.lang.IPersistentStack
  (seq [_this]
    (when (seq queue)
      (->> queue seq (map first))))

  (cons [_this o]
    (PriorityQueue. (assoc queue o (keyfn o)) keyfn))

  (empty [_this] (PriorityQueue. (priority-map/priority-map) keyfn))

  (equiv [_this o]
    (and (= queue (.queue o))
         (= keyfn (.keyfn o))))

  (peek [_this]
    (-> queue first first))

  (pop [_this]
    (PriorityQueue. (dissoc queue (first (first queue))) keyfn))

  clojure.lang.Counted
  (count [_this]
    (.count queue))

  clojure.lang.Reversible
  (rseq [_this]
    (when (seq queue)
      (->> queue rseq (map first))))

  clojure.lang.Sorted
  (comparator [this] (.comparator (.queue this)))

  (entryKey [_this o] (keyfn o))

  (seqFrom [this k ascending]
    (let [q (.queue this)]
      (->> (if ascending (subseq q >= k) (rsubseq q <= k))
           (map first))))

  (seq [this ascending]
    (let [q (.queue this)]
      (->> (if ascending (seq q) (rseq q))
           (map first)))))

(defmethod print-method PriorityQueue [pq ^java.io.Writer w]
  (.write w "#PriorityQueue")
  (.write w (pr-str (seq pq))))

(defn priority-queue
  "Creates a new priority queue where the priority is determined by applying
  `keyfn` on the data added. `keyfn` should return a number."
  ([keyfn] (priority-queue keyfn []))
  ([keyfn data]
   (PriorityQueue. (into (priority-map/priority-map) (map (fn [value] [value (keyfn value)]) data)) keyfn)))
