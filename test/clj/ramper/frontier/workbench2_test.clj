(ns ramper.frontier.workbench2-test
  (:require [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench2 :as workbench]
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
    (let [entry1 (-> (workbench/entry (url/scheme+authority "http://foo.bar") ["/path1" "/path2"])
                     (assoc :ip-address (first ip-addrs))
                     (assoc :next-fetch (util/from-now 300)))
          entry2 (-> (workbench/entry (url/scheme+authority "http://foo.bla") ["/path1" "/path2"])
                     (assoc :ip-address (second ip-addrs))
                     (assoc :next-fetch (util/from-now 200)))
          entry3 (-> (workbench/entry (url/scheme+authority "http://foo.toto") ["/path1" "/path2"])
                     (assoc :ip-address (first (nnext ip-addrs)))
                     (assoc :next-fetch (util/from-now 400)))
          entry4 (-> (workbench/entry (url/scheme+authority "http://foo.cofefe") ["/path1" "/path2"])
                     (assoc :ip-address (second ip-addrs))
                     (assoc :next-fetch (util/from-now 500)))
          wb (reduce workbench/add-entry (workbench/workbench) [entry1 entry2 entry3 entry4])]
      (is (= 4 (workbench/nb-workbench-entries wb)) "wrong number of workbench entries")
      (is (nil? (workbench/peek-entry wb)) "timestamps at beginning not honored")
      (is (true? (workbench/scheme+authority-present? wb (url/scheme+authority "http://foo.bar")))
          "missing scheme+authority")
      (Thread/sleep 200)
      (is (= entry2 (workbench/peek-entry wb))
          "incorrect first peeked entry")
      (Thread/sleep 100)
      (is (= entry1 (-> wb
                        workbench/pop-entry
                        workbench/peek-entry))
          "incorrect second peeked visit state")
      (Thread/sleep 100)
      (is (= entry3 (-> (apply-n workbench/pop-entry wb 2)
                        workbench/peek-entry))
          "incorrect third peeked visit state")
      (is (nil? (-> (apply-n workbench/pop-entry wb 3)
                    workbench/peek-entry))
          "workbench should be empty, as all entries are busy")
      (Thread/sleep 100)
      (let [entry2 (-> (workbench/peek-entry wb)
                       (assoc :next-fetch (util/from-now 200)))
            wb (-> wb
                   workbench/pop-entry
                   (workbench/add-entry entry2))]
        (is (= 4 (-> wb :entries count))
            "number of active workbench entries not correct")
        ;; we already popped once
        (is (= entry4 (-> (apply-n workbench/pop-entry wb 2)
                          workbench/peek-entry))
            "incorrect fourth peeked entry after readding of popped entry")
        (is (nil? (-> (apply-n workbench/pop-entry wb 3)
                      workbench/peek-entry))
            "workbench should be empty, as all entries are busy"))
      ;; same as the let above but with purge
      (let [entry2 (-> (workbench/peek-entry wb)
                       (assoc :next-fetch (util/from-now 200)))
            wb (-> wb
                   workbench/pop-entry
                   (workbench/purge-entry entry2))]
        (is (= 3 (-> wb :entries count))
            "number of active workbench entries not correct")
        ;; we already popped once
        (is (= entry4 (-> (apply-n workbench/pop-entry wb 2)
                          workbench/peek-entry))
            "incorrect fourth peeked entry after readding of popped entry")
        (is (nil? (-> (apply-n workbench/pop-entry wb 3)
                      workbench/peek-entry))
            "workbench should be empty, as all entries are busy")
        (is (false? (workbench/scheme+authority-present? wb (url/scheme+authority "http://foo.bla"))))))))

(deftest workbench-purge-test
  (testing "workbench purge testing"
    (let [ip (first ip-addrs)
          entry (-> (workbench/entry (url/scheme+authority "http://foo.bar"))
                    (assoc :ip-address ip)
                    (assoc :next-fetch (util/from-now 300)))
          wb (atom (-> (workbench/workbench)
                       (workbench/add-entry entry)
                       workbench/pop-entry))]
      (swap! wb workbench/purge-entry entry)
      (is (nil? (workbench/peek-entry @wb)))
      (is (zero? (workbench/nb-workbench-entries @wb))))))

(deftest workbench-atom-testing
  (testing "workbench with atom"
    (let [wb (atom (workbench/workbench))
          entry1 (-> (workbench/entry (url/scheme+authority "http://foo.bar") ["/path1" "/path2"])
                     (assoc :ip-address (first ip-addrs))
                     (assoc :next-fetch (util/from-now 300)))
          entry2 (-> (workbench/entry (url/scheme+authority "http://foo.bla") ["/path1" "/path2"])
                     (assoc :ip-address (second ip-addrs))
                     (assoc :next-fetch (util/from-now 200)))
          entry3 (-> (workbench/entry (url/scheme+authority "http://foo.toto") ["/path1" "/path2"])
                     (assoc :ip-address (first (nnext ip-addrs)))
                     (assoc :next-fetch (util/from-now 400)))
          entry4 (-> (workbench/entry (url/scheme+authority "http://foo.cofefe") ["/path1" "/path2"])
                     (assoc :ip-address (second ip-addrs))
                     (assoc :next-fetch (util/from-now 500)))
          _ (run! #(swap! wb workbench/add-entry %) [entry1 entry2 entry3 entry4])
          _ (Thread/sleep 200)
          entry2-dequeued (workbench/dequeue-entry! wb)
          _ (Thread/sleep 100)
          entry1-dequeued (workbench/dequeue-entry! wb)
          _ (Thread/sleep 100)
          entry3-dequeued (workbench/dequeue-entry! wb)
          _ (do
              (swap! wb workbench/add-entry (assoc entry2-dequeued :next-fetch (util/from-now 200)))
              (Thread/sleep 100))
          entry4-dequeued (workbench/dequeue-entry! wb)]
      (is (= entry1 (dissoc entry1-dequeued :locked-entry)))
      (is (= entry2 (dissoc entry2-dequeued :locked-entry)))
      (is (= entry3 (dissoc entry3-dequeued :locked-entry)))
      (is (= entry4 (dissoc entry4-dequeued :locked-entry)))
      (is (true? (workbench/scheme+authority-present? @wb (url/scheme+authority "http://foo.bar"))))
      #_(is (= (list vs1 vs2 vs3 vs4)
               (map #(dissoc % :locked-entry) (list vs1-dequeued vs2-dequeued vs3-dequeued vs4-dequeued)))))))
