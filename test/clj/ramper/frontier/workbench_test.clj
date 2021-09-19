(ns ramper.frontier.workbench-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.frontier.workbench.workbench-entry :as we]
            [ramper.util :as util]
            [ramper.util.url :as url])
  (:import (java.net InetAddress)))

(defn- get-address [host]
  (-> (InetAddress/getAllByName host)
      first))

(def ip-addrs (map get-address ["127.0.0.1" "127.0.0.2" "127.0.0.3"]))

(defn- apply-n [f x n]
  (->> (iterate f x)
       (drop n)
       first))

(deftest workbench-simple-test
  (testing "workbench testing"
    (let [vs1 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bar"))
                  (assoc :ip-address (first ip-addrs))
                  (assoc :next-fetch (util/from-now 300))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs2 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bla"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (util/from-now 200))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs3 (-> (visit-state/visit-state (url/scheme+authority "http://foo.toto"))
                  (assoc :ip-address (first (nnext ip-addrs)))
                  (assoc :next-fetch (util/from-now 400))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs4 (-> (visit-state/visit-state (url/scheme+authority "http://foo.cofefe"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (util/from-now 500))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          wb (reduce workbench/add-visit-state (workbench/workbench) [vs1 vs2 vs3 vs4])]
      (is (= 3 (workbench/nb-workbench-entries wb)) "wrong number of workbench entries")
      (is (nil? (workbench/peek-visit-state wb)) "timestamps at beginning not honored")
      (is (= 1  (-> (workbench/get-workbench-entry wb (first ip-addrs)) :visit-states count))
          "workbench entry contains the wrong number of visit states")
      (is (= 2  (-> (workbench/get-workbench-entry wb (second ip-addrs)) :visit-states count))
          "workbench entry contains the wrong number of visit states")
      (is (true? (workbench/scheme+authority-present? wb (url/scheme+authority "http://foo.bar")))
          "missing scheme+authority")
      (Thread/sleep 200)
      (is (= vs2 (workbench/peek-visit-state wb))
          "incorrect first peeked visit state")
      (Thread/sleep 100)
      (is (= vs1 (-> wb
                     workbench/pop-visit-state
                     workbench/peek-visit-state))
          "incorrect second peeked visit state")
      (Thread/sleep 100)
      (is (= vs3 (-> (apply-n workbench/pop-visit-state wb 2)
                     workbench/peek-visit-state))
          "incorrect third peeked visit state")
      (is (nil? (-> (apply-n workbench/pop-visit-state wb 3)
                    workbench/peek-visit-state))
          "workbench should be empty, as all entries are busy")
      (Thread/sleep 100)
      (let [vs2 (-> (workbench/peek-visit-state wb)
                    (assoc :locked-entry true)
                    (assoc :next-fetch (util/from-now 200)))
            wb (-> wb
                   workbench/pop-visit-state
                   (workbench/add-visit-state vs2))]
        (is (= 3 (-> wb :entries count))
            "number of active workbench entries not correct")
        ;; we already popped once
        (is (= vs4 (-> (apply-n workbench/pop-visit-state wb 2)
                       workbench/peek-visit-state))
            "incorrect fourth peeked visit state after readding of popped visit-state")
        (is (nil? (-> (apply-n workbench/pop-visit-state wb 3)
                      workbench/peek-visit-state))
            "workbench should be empty, as all entries are busy"))
      ;; same as the let above but with purge
      (let [vs2 (-> (workbench/peek-visit-state wb)
                    (assoc :locked-entry true)
                    (assoc :next-fetch (util/from-now 200)))
            wb (-> wb
                   workbench/pop-visit-state
                   (workbench/purge-visit-state vs2))]
        (is (= 3 (-> wb :entries count))
            "number of active workbench entries not correct")
        ;; we already popped once
        (is (= vs4 (-> (apply-n workbench/pop-visit-state wb 2)
                       workbench/peek-visit-state))
            "incorrect fourth peeked visit state after readding of popped visit-state")
        (is (nil? (-> (apply-n workbench/pop-visit-state wb 3)
                      workbench/peek-visit-state))
            "workbench should be empty, as all entries are busy")
        (is (false? (workbench/scheme+authority-present? wb (url/scheme+authority "http://foo.bla"))))))))

(deftest workbench-purge-test
  (testing "workbench purge testing"
    (let [ip (first ip-addrs)
          vs1 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bar"))
                  (assoc :ip-address ip)
                  (assoc :next-fetch (util/from-now 300)))
          wb (atom (-> (workbench/workbench)
                       (update :address-to-busy-entry assoc (workbench/hash-ip ip)
                               (we/workbench-entry ip))))]
      (swap! wb workbench/purge-visit-state vs1)
      (is (nil? (workbench/peek-visit-state @wb)))
      (is (zero? (workbench/nb-workbench-entries @wb))))))

(deftest workbench-atom-testing
  (testing "workbench with atom"
    (let [wb (atom (workbench/workbench))
          vs1 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bar"))
                  (assoc :ip-address (first ip-addrs))
                  (assoc :next-fetch (util/from-now 300))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs2 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bla"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (util/from-now 200))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs3 (-> (visit-state/visit-state (url/scheme+authority "http://foo.toto"))
                  (assoc :ip-address (first (nnext ip-addrs)))
                  (assoc :next-fetch (util/from-now 400))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs4 (-> (visit-state/visit-state (url/scheme+authority "http://foo.cofefe"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (util/from-now 500))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          _ (run! #(swap! wb workbench/add-visit-state %) [vs1 vs2 vs3 vs4])
          _ (Thread/sleep 200)
          vs2-dequeued (workbench/dequeue-visit-state! wb)
          _ (Thread/sleep 100)
          vs1-dequeued (workbench/dequeue-visit-state! wb)
          _ (Thread/sleep 100)
          vs3-dequeued (workbench/dequeue-visit-state! wb)
          _ (do
              (swap! wb workbench/add-visit-state (assoc vs2-dequeued :next-fetch (util/from-now 200)))
              (Thread/sleep 100))
          vs4-dequeued (workbench/dequeue-visit-state! wb)]
      (is (= vs1 (dissoc vs1-dequeued :locked-entry)))
      (is (= vs2 (dissoc vs2-dequeued :locked-entry)))
      (is (= vs3 (dissoc vs3-dequeued :locked-entry)))
      (is (= vs4 (dissoc vs4-dequeued :locked-entry)))
      (is (true? (workbench/scheme+authority-present? @wb (url/scheme+authority "http://foo.bar"))))
      #_(is (= (list vs1 vs2 vs3 vs4)
               (map #(dissoc % :locked-entry) (list vs1-dequeued vs2-dequeued vs3-dequeued vs4-dequeued)))))))
