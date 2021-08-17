(ns ramper.util.data-disk-queues-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.util :as util]
            [ramper.util.data-disk-queues :as ddq]
            [ramper.util.url :as url]
            [ramper.util.url-factory :as url-factory]))

(defn- scheme+authority-key [url]
  (-> url url/scheme+authority str hash))

(deftest simple-data-disk-queues-test
  (testing "data disk queues single-threaded with 2 scheme+authority pairs"
    (let [ddq (ddq/data-disk-queues (util/temp-dir "data-disk-queues"))
          nb-items 2
          urls1(-> (url-factory/rand-scheme+authority-seq nb-items) distinct)
          urls2 (->> (url-factory/rand-scheme+authority-seq nb-items) distinct (take (count urls1)))
          key1 (-> urls1 first scheme+authority-key)
          key2 (-> urls2 first scheme+authority-key)]
      (loop [urls1 urls1 urls2 urls2]
        (when (seq urls1)
          (ddq/enqueue ddq key1 (first urls1))
          (ddq/enqueue ddq key2 (first urls2))
          (recur (rest urls1) (rest urls2))))
      (is (= (count urls1) (ddq/count ddq key1)))
      (is (= (count urls2) (ddq/count ddq key2)))
      (is (= 2 (ddq/num-keys ddq)))
      (let [[urls1-dequeued urls2-dequeued]
            (loop [urls1-rev '() urls2-rev '()]
              (if-not (pos? (ddq/count ddq key1))
                [(reverse urls1-rev) (reverse urls2-rev)]
                (recur (cons (ddq/dequeue ddq key1) urls1-rev)
                       (cons (ddq/dequeue ddq key2) urls2-rev))))]
        (is (= urls1 urls1-dequeued))
        (is (= urls2 urls2-dequeued))))))
