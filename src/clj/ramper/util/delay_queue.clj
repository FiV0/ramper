(ns ramper.util.delay-queue
  "A simple wrapper around `clojure.data.priority-map` to simulate delays.

  Values are enqueued as `[item time]` pairs where `time` is a timestamp in
  milliseconds. A usage example

  Constructing a delay queue with the function delay-queue
  user=> (def dq (atom (delay-queue)))
  #'user/dq

  We can conj an item plus its availability time.
  user=> (swap! dq conj [:foo (+ (System/currentTimeMillis) 3000)])
  #object[ramper.util.delay_queue.DelayQueue 0x344ab585 \"clojure.lang.LazySeq@3cd1fc84\"]

  The item won't be immediately available.
  user=> (peek @dq)
  nil

  But if we wait a little...
  user=> (peek @dq)
  :foo

  The underlying atom has not changed so it's still available via dequeue!. This should
  also be the standard way of dequeuing items when the queue is accessed concurrently.
  user=> (dequeue! dq)
  :foo

  With dequeue! one assures that an item will only be popped once.
  user=> (dequeue! dq)
  nil

  It's important to not enqueue `nil` values as otherwise the semantics
  of the queue will fail."
  (:require [clojure.data.priority-map :as priority-map]))

;; TODO maybe look into fitting it into Clojure interfaces

(deftype DelayQueue [priority-queue]
  Object
  (toString [this] (str (.seq this)))

  clojure.lang.IPersistentStack
  (seq [_this]
    (->> priority-queue seq (map first)))

  (cons [_this o]
    (DelayQueue. (assoc priority-queue (first o) (second o))))

  (empty [_this] (DelayQueue. (priority-map/priority-map)))

  (equiv [_this o]
    (= priority-queue (.priority-queue ^DelayQueue o)))

  (peek [_this]
    (when-let [entry (peek priority-queue)]
      (if (<= (second entry) (System/currentTimeMillis))
        (first entry)
        nil)))

  (pop [this]
    (if-let [entry (peek priority-queue)]
      (if (<= (second entry) (System/currentTimeMillis))
        (DelayQueue. (pop priority-queue))
        this)
      this))

  clojure.lang.Counted
  (count [_this]
    (count priority-queue)))

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
            (compare-and-set! delay-queue-atom q nq) value
            :else (recur)))))

(defn delay-queue
  ([] (delay-queue []))
  ([data]
   (let [now (System/currentTimeMillis)]
     (DelayQueue. (into (priority-map/priority-map) (map #(vector % now) data))))))

(comment
  (def dq (atom (delay-queue)))

  (swap! dq conj [:foo (+ (System/currentTimeMillis) 3000)]) ; 3 seconds
  (peek @dq);; => nil
  (count @dq);; => 1

  ;; wait a little
  (peek @dq);; => :foo
  ;; modifying the underlying atom
  (dequeue! dq);; => :foo
  (dequeue! dq);; => nil
  )
