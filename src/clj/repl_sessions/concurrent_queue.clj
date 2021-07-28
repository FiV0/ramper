(ns repl-sessions.concurrent-queue
  (:require [clojure.core.async :as async]
            [com.manigfeald.queue :as queue]
            [ramper.util :as util])
  (:import (java.util.concurrent ConcurrentLinkedQueue)))

(def nb-threads (util/number-of-cores))
(def approx-number-items 5000000)
(def number-items (+ (* (quot approx-number-items nb-threads) nb-threads) nb-threads))
(def items-per-thread (/ number-items nb-threads))

;; ConcurrentLinkedQueue
(let [start (System/currentTimeMillis)
      queue (ConcurrentLinkedQueue.)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (dotimes [_ items-per-thread]
                                               (.offer queue (util/rand-str 12)))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (cond
                                                (= cnt items-per-thread) true
                                                (.poll queue) (recur (inc cnt))
                                                :else (recur cnt))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 6.766

;; queue with atomic reference
(let [start (System/currentTimeMillis)
      queue (queue/queue)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (dotimes [_ items-per-thread]
                                               (queue/enqueue queue (util/rand-str 12)))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (cond
                                                (= cnt items-per-thread) true
                                                (queue/dequeue queue) (recur (inc cnt))
                                                :else (recur cnt))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 7.022

;; persistent queue with atom and swap
;; this test is kind of flawed as you are not actually getting the top element
;; but only dequeueing we need to use some sort of dequeue algo, see below
(let [start (System/currentTimeMillis)
      queue (atom clojure.lang.PersistentQueue/EMPTY)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (dotimes [_ items-per-thread]
                                               (swap! queue conj (util/rand-str 12)))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (when (= cnt items-per-thread)
                                                (swap! queue pop)
                                                (recur (inc cnt)))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 7.731

(defn dequeue! [queue]
  (loop []
    (let [q     @queue
          value (peek q)
          nq    (pop q)]
      (if (compare-and-set! queue q nq)
        value
        (recur)))))

(let [start (System/currentTimeMillis)
      queue (atom clojure.lang.PersistentQueue/EMPTY)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (dotimes [_ items-per-thread]
                                               (swap! queue conj (util/rand-str 12)))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (when (= cnt items-per-thread)
                                                (dequeue! queue)
                                                (recur (inc cnt)))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 7.721
;; wow still quite fast

(defn dequeue2! [queue] (ffirst (swap-vals! queue pop)))

;; using simpler dequeue!
(let [start (System/currentTimeMillis)
      queue (atom clojure.lang.PersistentQueue/EMPTY)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (dotimes [_ items-per-thread]
                                               (swap! queue conj (util/rand-str 12)))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (when (= cnt items-per-thread)
                                                (dequeue2! queue)
                                                (recur (inc cnt)))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 7.553

;; using a channel
(let [start (System/currentTimeMillis)
      queue (async/chan 5000000)
      ;; the doall is important otherwise we get bitten by laziness
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (loop [cnt 0]
                                               (when (< cnt items-per-thread)
                                                 (let [t (async/timeout 500)
                                                       v (async/alt!! t :timeout
                                                                      [[queue (util/rand-str 12)]] :put)]
                                                   (if (= v :timeout)
                                                     (recur cnt)
                                                     (recur (inc cnt))))))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (when (< cnt items-per-thread)
                                                (let [t (async/timeout 500)
                                                      v (async/alt!! t :timeout
                                                                     queue :taken)]
                                                  (if (= v :timeout)
                                                    (recur cnt)
                                                    (recur (inc cnt))))))
                                            true)))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 25.047
;; not very good even with fairly large buffer

(let [start (System/currentTimeMillis)
      queue (async/chan 5000000)
      enqueue-threads (doall (repeatedly  nb-threads
                                          #(async/thread
                                             (loop [cnt 0]
                                               (when (< cnt items-per-thread)
                                                 (if (async/offer! queue (util/rand-str 12))
                                                   (recur (inc cnt))
                                                   (recur cnt)
                                                   )))
                                             true)))
      dequeue-threads (doall (repeatedly nb-threads
                                         #(async/thread
                                            (loop [cnt 0]
                                              (when (< cnt items-per-thread)
                                                (if (async/poll! queue)
                                                  (recur (inc cnt))
                                                  (recur cnt))))
                                            true)))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 32.54
;; even worse with poll!/offer!
