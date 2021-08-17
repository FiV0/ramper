(ns ramper.sieve
  (:refer-clojure :exclude [flush]))

;;;; NOTICE
;; The protocols and sieve implementations follow closely the ones described by BUBing.

(defprotocol Sieve
  "A Sieve guarantees the following property: every key that is enqueued gets dequeued once,
  and once only. It sort of works like a unique filter."
  (enqueue [this key] "Add the given key to the sieve.")
  (flush [this] "Flushes all pending enqueued keys to the flow-receiver.")
  (last-flush [this] "Returns a timestamp of the last sieve flush."))

(defprotocol Size
  "Generic protocol to get the size (number of items) in the data structure."
  (number-of-items [this]))
