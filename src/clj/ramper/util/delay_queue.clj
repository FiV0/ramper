(ns ramper.util.delay-queue
  "A simple wrapper around `clojure.data.priority-map` to simulate delays.

  It's important to not enqueue `nil` values as otherwise the semantics
  of the queue will fail."
  (:refer-clojure :exclude [peek pop assoc])
  (:require [clojure.data.priority-map :as priority-map]))

;; TODO maybe look into fitting it into Clojure interfaces

(defrecord DelayQueue [priority-queue]
  Object
  (toString [this] (str (.seq this))))

(defn peek
  "Returns the item with the smallest delay in the queue. `nil`
  if none is available."
  [{:keys [priority-queue]  :as _delay-queue}]
  (when-let [entry (clojure.core/peek priority-queue)]
    (if (<= (second entry) (System/currentTimeMillis))
      (first entry)
      nil)))

(defn pop
  "Returns the delay-queue updated if there was an item to be poped."
  [{:keys [priority-queue] :as delay-queue}]
  (if-let [entry (clojure.core/peek priority-queue)]
    (if (<= (second entry) (System/currentTimeMillis))
      (update delay-queue :priority-queue clojure.core/pop)
      delay-queue)
    delay-queue))

(defn assoc
  "Assocs the corresponding `item` + `time` onto the delay-queue."
  [delay-queue item time]
  (update delay-queue :priority-queue clojure.core/assoc item time))

(defn dequeue!
  "Takes an atom containing a delay queue and pops (if possible) the first
  value also assuring that the underlying queue has not changed since the pop.
  Returns the popped element if any, nil if no value is available."
  [delay-queue-atom]
  (loop []
    (let [q     @delay-queue-atom
          value (peek q)
          nq    (pop q)]
      (cond (nil? value) nil
            (and value (compare-and-set! delay-queue-atom q nq)) value
            :else (recur)))))

(defn delay-queue
  ([] (delay-queue []))
  ([data]
   (let [now (System/currentTimeMillis)]
     (DelayQueue. (into (priority-map/priority-map) (map #(vector % now) data))))))

(comment
  (def d 3000)
  (def dq (atom (delay-queue)))

  (swap! dq assoc :foo (+ (System/currentTimeMillis) d))
  (peek @dq)

  ;; wait a little
  (peek @dq)

  (dequeue! dq))
