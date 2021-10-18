(ns ramper.frontier.workbench.virtualizer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench.virtualizer :as virtualizer]
            [ramper.util :as util]
            [ramper.util.url :as url]
            [ramper.util.url-factory :as url-factory])
  (:import (ramper.frontier Entry)))

(defn dequeue-entry [entry]
  (for [_ (range (.size entry))]
    (.getPathQuery entry)))

(deftest simple-virtualizer-test
  (let [virtual (virtualizer/workbench-virtualizer (util/temp-dir "virtualizer"))
        sa1 (-> (url-factory/rand-scheme+authority-seq 1000) distinct)
        sa2 (->> (url-factory/rand-scheme+authority-seq 1000) (take (count sa1)))
        entry1 (Entry. (-> sa1 first url/base str))
        entry2 (Entry. (-> sa2 first url/base str))]
    (testing "virtualizer single-threaded with 2 scheme+authority pairs"
      (loop [sa1 sa1 sa2 sa2]
        (when (seq sa1)
          (virtualizer/enqueue virtual entry1 (first sa1))
          (virtualizer/enqueue virtual entry2 (first sa2))
          (recur (rest sa1) (rest sa2))))
      (is (= (virtualizer/count virtual entry1) (count sa1)))
      (is (= (virtualizer/count virtual entry2) (count sa2)))
      (loop [entry1 entry1 entry2 entry2]
        (if (pos? (virtualizer/count virtual entry1))
          (recur (virtualizer/dequeue-path-queries virtual entry1 100)
                 (virtualizer/dequeue-path-queries virtual entry2 100))
          (do
            (is (= (map #(-> % url/path+queries str) sa1)
                   (dequeue-entry entry1)))
            (is (= (map #(-> % url/path+queries str) sa2)
                   (dequeue-entry entry2)))))))
    (testing "virtualizer object equality"
      ;; here we explicitly create the visit state twice
      (virtualizer/enqueue virtual (Entry. "https://httpbin.org") "https://httpbin.org/get")
      (let [new-entry (virtualizer/dequeue-path-queries
                       virtual
                       (Entry. "https://httpbin.org")
                       1)]
        (is (= "/get" (.getPathQuery new-entry)))))))
