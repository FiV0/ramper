(ns ramper.sieve
  (:refer-clojure :exclude [flush]))

;;;; NOTICE
;; The protocols and sieve implementations follow closely the ones described by BUBing.

(defprotocol FlowReceiver
  "The FlowReceiver protocol should be implemented by the
  receiver of the new keys that come out of the sieve. This acts
  sort of as a listener for keys that make it through the seive."
  (prepare-to-append [this] "A new flow of keys is ready to be appended to this receiver.")
  (append [this hash key] "A new key is appended")
  (finish-appending [this] "The new flow of keys has finished")
  (no-more-append [this] "No more appends will happen as the underlying sieve was closed."))

(defprotocol Sieve
  "A Sieve guarantees the following property: every key that is enqueued gets dequeued once,
  and once only. It sort of works like a unique filter."
  (enqueue [this key] "Add the given key to the sieve.")
  (flush [this] "Flushes all pending enqueued keys to the flow-receiver.")
  (last-flush [this] "Returns a timestamp of the last sieve flush."))

(defprotocol Size
  "Generic protocol to get the size (number of items) in the data structure."
  (number-of-items [this]))
