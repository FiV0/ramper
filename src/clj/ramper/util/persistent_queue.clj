(ns ramper.util.persistent-queue
  "Utility functions for `clojure.lang.PersistentQueue`")

;; Copied from
;; https://stackoverflow.com/questions/8938330/clojure-swap-atom-dequeuing

(defn dequeue!
  "Takes an atom containing a `clojure.lang.PersistentQueue` and pops the first
  value also assuring that the underlying queue has not changed since the pop.
  Returns the popped element if there is one, nil otherwise."
  [queue-atom] (ffirst (swap-vals! queue-atom pop)))

(defmethod print-method clojure.lang.PersistentQueue [pq ^java.io.Writer w]
  (.write w "#PersistentQueue")
  (.write w (pr-str (seq pq))))
