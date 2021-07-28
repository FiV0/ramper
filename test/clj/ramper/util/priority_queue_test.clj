(ns ramper.util.priority-queue-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.util.priority-queue :as priority-queue]))

(deftest priority-queue-basic-ops
  (let [pq (priority-queue/priority-queue (fn [item] (:prio item)) [{:prio 1 :data :foo} {:prio 2 :data :bar}])
        empty-pq (priority-queue/priority-queue :prio)]
    (testing "basic ops"
      (let [pq (conj pq {:prio 3 :data :bla})]
        (is (= '({:prio 1 :data :foo} {:prio 2 :data :bar} {:prio 3 :data :bla}) (seq pq))
            "seq not working")
        (is (= 3 (count pq))
            "count not working")
        (is (= (reverse '({:prio 1 :data :foo} {:prio 2 :data :bar} {:prio 3 :data :bla})) (rseq pq))
            "rseq not working")
        (is (= '({:prio 1 :data :foo}) (subseq pq < 2))
            "subseq less then not working")
        (is (= '({:prio 2 :data :bar} {:prio 3 :data :bla}) (subseq pq >= 2))
            "subseq greater or equal then not working")
        (is (= '({:prio 2 :data :bar} {:prio 1 :data :foo}) (rsubseq pq < 3))
            "rsubseq less then not working")
        (is (= '({:prio 3 :data :bla} {:prio 2 :data :bar}) (rsubseq pq >= 2))
            "rsubseq greater or equal then not working")
        (is (nil? (first empty-pq))
            "first not working")
        (is (= '() (rest empty-pq))
            "rest not working")
        (is (= {:prio 1 :data :foo} (peek pq))
            "peek not working")
        (is (= '({:prio 2 :data :bar} {:prio 3 :data :bla}) (-> pq pop seq))
            "pop not working")))))

(deftest priority-queue-map-interface
  (let [pq (priority-queue/priority-queue :prio [{:prio 1 :data :foo} {:prio 2 :data :bar}])]
    (testing "assoc and dissoc for priority queue"
      (is (= '({:prio 1 :data :foo} {:prio 2 :data :bar} {:prio 3 :data :bla})
             (-> pq
                 (assoc {:prio 3 :data :bla} 3)
                 seq))
          "assoc not working")
      (is (= '({:prio 1 :data :foo})
             (seq (dissoc pq {:prio 2 :data :bar})) )
          "dissoc not working")
      #_(is (= '({:prio 2 :data :bar} {:prio 3 :data :foo})
               (update pq {:prio 1 :data :foo} #(update % :prio (fn [prio] (+ prio 2)))))))))
