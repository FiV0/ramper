(ns ramper.util.thread-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.util.thread :as threads-util]))

(defn- my-thread-fn [put-chan stop-chan]
  (loop [i 0]
    (if-not (async/poll! stop-chan)
      (do
        (Thread/sleep 100)
        (async/>!! put-chan i)
        (recur (inc i)))
      true)))

(defn- my-bad-thread-fn [_stop-chan]
  true)

(deftest threads-util-test
  (testing "ramper.util.threads/ThreadWrapper"
    (let [put-chan (async/chan)
          t-wrapper (threads-util/thread-wrapper (partial my-thread-fn put-chan))]
      (is (= '(0 1 2) (repeatedly 3 #(async/<!! put-chan))))
      (async/close! put-chan)
      (threads-util/stop t-wrapper)
      (is (true? (threads-util/stopped? t-wrapper)))))
  (testing "ramper.util.threads/ThreadWrapper with incorrect thread-fn"
    (let [t-wrapper (threads-util/thread-wrapper my-bad-thread-fn)]
      (threads-util/stop t-wrapper)
      (is (true? (threads-util/stopped? t-wrapper))))))
