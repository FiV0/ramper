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
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (dotimes [_ items-per-thread]
                                        (.offer queue (util/rand-str 12)))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (cond
                                         (= cnt items-per-thread) true
                                         (.poll queue) (recur (inc cnt))
                                         :else (recur cnt)))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 17.052

;; queue with atomic reference
(let [start (System/currentTimeMillis)
      queue (queue/queue)
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (dotimes [_ items-per-thread]
                                        (queue/enqueue queue (util/rand-str 12)))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (cond
                                         (= cnt items-per-thread) true
                                         (queue/dequeue queue) (recur (inc cnt))
                                         :else (recur cnt)))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 24.954

;; persistent queue with atom and swap
;; this test is kind of flawed as you are not actually getting the top element
;; but only dequeueing we need to use some sort of dequeue algo, see below
(let [start (System/currentTimeMillis)
      queue (atom clojure.lang.PersistentQueue/EMPTY)
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (dotimes [_ items-per-thread]
                                        (swap! queue conj (util/rand-str 12)))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (when (= cnt items-per-thread)
                                         (swap! queue pop)
                                         (recur (inc cnt))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 10.861

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
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (dotimes [_ items-per-thread]
                                        (swap! queue conj (util/rand-str 12)))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (when (= cnt items-per-thread)
                                         (dequeue! queue)
                                         (recur (inc cnt))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 10.505
;; wow still extremely fast

(defn dequeue2! [queue] (ffirst (swap-vals! queue pop)))

;; using simpler dequeue!
(let [start (System/currentTimeMillis)
      queue (atom clojure.lang.PersistentQueue/EMPTY)
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (dotimes [_ items-per-thread]
                                        (swap! queue conj (util/rand-str 12)))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (when (= cnt items-per-thread)
                                         (dequeue2! queue)
                                         (recur (inc cnt))))))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
;; => 10.434

;; using a channel
;; this is broken
(let [start (System/currentTimeMillis)
      queue (async/chan 1000)
      enqueue-threads (repeatedly  nb-threads
                                   #(async/thread
                                      (loop [cnt 0]
                                        (when (< cnt items-per-thread)
                                          ;; (when (= 0 (mod cnt 100000))
                                          ;;   (println "foo bar"))
                                          (let [t (async/timeout 500)
                                                v (async/alt!! t :timeout
                                                               [[queue (util/rand-str 12)]] :put)]
                                            (if (= v :timeout)
                                              (do
                                                (Thread/sleep 100)
                                                (recur cnt))
                                              (recur (inc cnt))))))
                                      true))
      dequeue-threads (repeatedly nb-threads
                                  #(async/thread
                                     (loop [cnt 0]
                                       (when (< cnt items-per-thread)
                                         (let [t (async/timeout 500)
                                               v (async/alt!! t :timeout
                                                              queue :taken)]
                                           (if (= v :timeout)
                                             (do
                                               (Thread/sleep 100)
                                               (recur cnt))
                                             (recur (inc cnt))))))
                                     true))]
  (run! async/<!! enqueue-threads)
  (run! async/<!! dequeue-threads)
  (double (/ (- (System/currentTimeMillis) start) 1000)))
