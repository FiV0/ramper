(ns ramper.util.lru-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.util.threads :as threads-util]))

(defn- my-thread-fn [put-chan stop-chan]
  (loop [i 0]
    (if-not (async/poll! stop-chan)
      (do
        (Thread/sleep 1000)
        (async/>!! put-chan i)
        (recur (inc i)))
      true)))

(deftest threads-util-test
  (testing "ramper.util.threads/ThreadWrapper"
    (let [put-chan (async/chan)
          t-wrapper (threads-util/thread-wrapper (partial my-thread-fn put-chan))]
      (is (= '(0 1 2) (repeatedly 3 #(async/<!! put-chan))))
      (async/close! put-chan)
      (is (true? (threads-util/stop t-wrapper))))))
