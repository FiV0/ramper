(ns repl-sessions.concurrent-queue
  (:require [clojure.core.async :as async]
            [criterium.core :as criterium]
            [com.manigfeald.queue :as queue]
            [ramper.util :as util])
  (:import (java.util.concurrent ConcurrentLinkedQueue)))

(def nb-threads 256;(* 3 (util/number-of-cores))
  )
(def approx-number-items 2000000)
(def number-items (+ (* (quot approx-number-items nb-threads) nb-threads) nb-threads))
(def items-per-thread (/ number-items nb-threads))

;; ConcurrentLinkedQueue
(defn test-stateful-queue []
  (let [queue (ConcurrentLinkedQueue.)
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
    (run! async/<!! dequeue-threads)))

(let [start (System/currentTimeMillis)]
  (test-stateful-queue)
  (double (/ (- (System/currentTimeMillis) start) 1000)))

(criterium/with-progress-reporting (criterium/bench (test-stateful-queue)))
;; => mean 2.78 sec


;; queue with atomic reference
(defn test-queue-with-atomic-ref []
  (let [queue (queue/queue)
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
    (run! async/<!! dequeue-threads)))

(let [start (System/currentTimeMillis)]
  (test-queue-with-atomic-ref)
  (double (/ (- (System/currentTimeMillis) start) 1000)))

(criterium/with-progress-reporting (criterium/bench (test-stateful-queue)))
;; => mean 2.76 sec

;; persistent queue with atom and swap
;; this test is kind of flawed as you are not actually getting the top element
;; but only dequeueing we need to use some sort of dequeue algo, see below
(defn persistent-queue-with-atom-and-swap []
  (let [queue (atom clojure.lang.PersistentQueue/EMPTY)
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
    (run! async/<!! dequeue-threads)))

(let [start (System/currentTimeMillis)]
  (persistent-queue-with-atom-and-swap)
  (double (/ (- (System/currentTimeMillis) start) 1000)))

(criterium/with-progress-reporting (criterium/bench (persistent-queue-with-atom-and-swap)))

(defn dequeue! [queue]
  (loop []
    (let [q     @queue
          value (peek q)
          nq    (pop q)]
      (if (compare-and-set! queue q nq)
        value
        (recur)))))

(defn persistent-queue-with-dequeue! []
  (let [queue (atom clojure.lang.PersistentQueue/EMPTY)
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
    (run! async/<!! dequeue-threads)))

(let [start (System/currentTimeMillis)]
  (persistent-queue-with-dequeue!)
  (double (/ (- (System/currentTimeMillis) start) 1000)))

(criterium/with-progress-reporting (criterium/bench (persistent-queue-with-dequeue!)))
;; mean 3.61 sec

(defn dequeue2! [queue] (ffirst (swap-vals! queue pop)))

(defn persistent-queue-with-dequeue2! []
  (let [queue (atom clojure.lang.PersistentQueue/EMPTY)
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
    (run! async/<!! dequeue-threads)))

;; using simpler dequeue!
(let [start (System/currentTimeMillis)]
  (persistent-queue-with-dequeue2!)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 3.602

(criterium/with-progress-reporting (criterium/bench (persistent-queue-with-dequeue2!)))
;; mean 3.64 sec

;; using a channel
(let [start (System/currentTimeMillis)
      queue (async/chan 2000000)
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
;; => 10.732
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
;; even worse with poll!/offer!
