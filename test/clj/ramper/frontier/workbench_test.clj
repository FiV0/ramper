(ns ramper.frontier.workbench-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.workbench-entry :as we]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.util.url :as url])
  (:import (java.net InetAddress)))

(defn- get-bytes [host]
  (-> (InetAddress/getAllByName host)
      first
      .getAddress))

(defn from-now [millis]
  (+ (System/currentTimeMillis) millis))

(def ip-addrs (map get-bytes ["127.0.0.1" "127.0.0.2" "127.0.0.3"]))

(defn- apply-n [f x n]
  (->> (iterate f x)
       (drop n)
       first))

(deftest workbench-simple-test
  (testing "workbench testing"
    (let [vs1 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bar"))
                  (assoc :ip-address (first ip-addrs))
                  (assoc :next-fetch (from-now 300))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs2 (-> (visit-state/visit-state (url/scheme+authority "http://foo.bla"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (from-now 200))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs3 (-> (visit-state/visit-state (url/scheme+authority "http://foo.toto"))
                  (assoc :ip-address (first (nnext ip-addrs)))
                  (assoc :next-fetch (from-now 400))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs4 (-> (visit-state/visit-state (url/scheme+authority "http://foo.cofefe"))
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (from-now 500))
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
                    (assoc :next-fetch (from-now 200)))
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
                    (assoc :next-fetch (from-now 200)))
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
