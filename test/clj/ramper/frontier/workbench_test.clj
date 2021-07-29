(ns ramper.frontier.workbench-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.workbench-entry :as wb-entry]
            [ramper.frontier.workbench.visit-state :as visit-state])
  (:import (java.net InetAddress)))

(defn- get-bytes [host]
  (-> (InetAddress/getAllByName host)
      first
      .getAddress))

(defn from-now [millis]
  (+ (System/currentTimeMillis) millis))

(def ip-addrs (map get-bytes ["127.0.0.1" "127.0.0.2" "127.0.0.3"]))

(deftest workbench-simple-test
  (testing "simple workbench testing"
    (let [wb-entry (-> (wb-entry/workbench-entry (first ip-addrs))
                       (assoc :next-fetch (from-now 100)))
          vs1 (-> (visit-state/visit-state "http://foo.bar")
                  (assoc :ip-address (first ip-addrs))
                  (assoc :next-fetch (from-now 300))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs2 (-> (visit-state/visit-state "http://foo.bla")
                  (assoc :ip-address (second ip-addrs))
                  (assoc :next-fetch (from-now 200))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          vs3 (-> (visit-state/visit-state "http://foo.toto")
                  (assoc :ip-address (first (nnext ip-addrs)))
                  (assoc :next-fetch (from-now 400))
                  (visit-state/enqueue-path-query "/path1")
                  (visit-state/enqueue-path-query "/path2"))
          wb (-> (workbench/workbench)
                 (workbench/add-workbench-entry wb-entry)
                 (workbench/add-visit-state vs1)
                 (workbench/add-visit-state vs2)
                 (workbench/add-visit-state vs3))]
      (is (= 3 (workbench/nb-workbench-entries wb)))
      (is (= nil (workbench/peek-visit-state wb)))
      (Thread/sleep 200)
      (is (= vs2 (workbench/peek-visit-state wb)))
      (Thread/sleep 100)
      (is (= vs1 (-> wb
                     workbench/pop-visit-state
                     workbench/peek-visit-state)))
      (Thread/sleep 100)
      (is (= vs3 (-> wb
                     workbench/pop-visit-state
                     workbench/pop-visit-state
                     workbench/peek-visit-state))))))
