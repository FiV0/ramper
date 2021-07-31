(ns ramper.frontier.workbench.virtualizer-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench.virtualizer :as virtualizer]
            [ramper.frontier.workbench.visit-state :as vs]
            [ramper.util :as util]
            [ramper.util.url :as url]
            [ramper.util.url-factory :as url-factory]))

(deftest simple-virtualizer-test
  (testing "virtualizer single-threaded with 2 scheme+authority pairs"
    (let [virtual (virtualizer/workbench-virtualizer (util/temp-dir "virtualizer"))
          sa1 (-> (url-factory/rand-scheme+authority-seq 1000) distinct)
          sa2 (->> (url-factory/rand-scheme+authority-seq 1000) (take (count sa1)))
          vs1 (vs/visit-state (-> sa1 first url/base str))
          vs2 (vs/visit-state (-> sa2 first url/base str))]
      (loop [sa1 sa1 sa2 sa2]
        (when (seq sa1)
          (virtualizer/enqueue virtual vs1 (first sa1))
          (virtualizer/enqueue virtual vs2 (first sa2))
          (recur (rest sa1) (rest sa2))))
      (is (= (virtualizer/count virtual vs1) (count sa1)))
      (is (= (virtualizer/count virtual vs2) (count sa2)))
      (loop [vs1 vs1 vs2 vs2]
        (if (pos? (virtualizer/count virtual vs1))
          (recur (virtualizer/dequeue-path-queries virtual vs1 100)
                 (virtualizer/dequeue-path-queries virtual vs2 100))
          (do
            (is (= (map #(-> % url/path+queries str) sa1)
                   (-> vs1 :path-queries seq)))
            (is (= (map #(-> % url/path+queries str) sa2)
                   (-> vs2 :path-queries seq)))))))))
