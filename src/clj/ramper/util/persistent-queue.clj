(ns ramper.util.persistent-queue
  "Utility functions for `clojure.lang.PersistentQueue`")

;; Copied from
;; https://stackoverflow.com/questions/8938330/clojure-swap-atom-dequeuing

(defn dequeue!
  "Takes an atom containing a `clojure.lang.PersistentQueue` and pops the first
  value also assuring that the underlying queue has not changed since the pop.
  Returns the poped element."
  [queue-atom]
  (loop []
    (let [q     @queue-atom
          value (peek q)
          nq    (pop q)]
      (if (compare-and-set! queue-atom q nq)
        value
        (recur)))))
