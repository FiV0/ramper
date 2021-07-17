(ns lock-free-queue
  "A thin rapper around `java.util.concurrent.ConcurrentLinkedQueue` so that
  count returns in constant time an approximate size."
  (:import java.util.concurrent.ConcurrentLinkedQueue
           java.util.concurrent.atomic.AtomicLong))

(defrecord LockFreeQueue [queue size])

(defn ->lock-free-queue
  "Constructor for the lock free queue."
  (^LockFreeQueue [^java.util.Collection coll]
   (->LockFreeQueue (ConcurrentLinkedQueue. coll) (AtomicLong. (count coll))))
  (^LockFreeQueue []
   (->LockFreeQueue (ConcurrentLinkedQueue.) (AtomicLong.))))

(defprotocol Queue
  (add ^Boolean [q e])
  (poll [q])
  (size ^Long [q]))

(extend-type LockFreeQueue
  Queue
  (add [q e]
    (.incrementAndGet (:size q))
    (.add (:queue q) e))
  (poll [q]
    (let [res (.poll (:queue q))]
      (when res (.decrementAndGet (:size q)))
      res))
  (size [q]
    (.get (:size q))))


(comment
  (def q (->lock-free-queue [1]))
  (add q 2)
  (size q)
  (poll q)
  (poll q)
  (poll q)
  )
