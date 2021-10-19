(ns ramper.workers.done-thread-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.frontier.workbench.workbench-entry :as we]
            [ramper.util :as util]
            [ramper.util.url :as url]
            [ramper.workers.done-thread :as done-thread])
  (:import (java.net InetAddress)
           (ramper.frontier Workbench3)))

(defn- create-dummy-ip [s]
  (let [ba (byte-array 4)]
    (doall (map-indexed #(aset-byte ba %1 %2) s))
    (InetAddress/getByAddress ba)))

(deftest done-thread-test
  (testing "done-thread"
    (let [dummy-ip1 (create-dummy-ip [1 2 3 4])
          dummy-ip2 (create-dummy-ip [2 3 4 5])
          dummy-ip3 (create-dummy-ip [3 4 5 6])
          url1 "https://finnvolkel.com"
          url2 "https://finnvolkel2.com"
          url3 "https://clojure.org"
          url4 "https://news.ycombinator.com"
          wb (Workbench3.)
          not-empty-entry (doto (.getEntry wb url1)
                            (.setNextFetch (util/from-now 40))
                            (.setIpAddress dummy-ip1)
                            (.addPathQuery "/foo/bar")
                            (.addPathQuery "/foo/bla"))
          still-in-wb-entry (doto (.getEntry wb url2)
                              (.setNextFetch (util/from-now 10))
                              (.setIpAddress dummy-ip1)
                              (.addPathQuery "/foo/bar")
                              (.addPathQuery "/foo/bla"))
          _ (.addEntry wb still-in-wb-entry)
          empty-entry (doto (.getEntry wb url3)
                        (.setNextFetch (util/from-now 30))
                        (.setIpAddress dummy-ip2))
          purgeable-entry (doto (.getEntry wb url4)
                            (.setNextFetch (util/from-now 20))
                            (.setIpAddress dummy-ip3))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "done-thread-test"))
          _ (do (virtual/enqueue virtualizer empty-entry "/foo/bar")
                (virtual/enqueue virtualizer empty-entry "/foo/bla"))
          runtime-config (atom {:ramper/runtime-stop false})
          done-queue (atom (into clojure.lang.PersistentQueue/EMPTY [not-empty-entry empty-entry purgeable-entry]))
          refill-queue (atom clojure.lang.PersistentQueue/EMPTY)
          thread-data {:workbench wb
                       :runtime-config runtime-config
                       :done-queue done-queue
                       :refill-queue refill-queue
                       :virtualizer virtualizer}
          thread (async/thread (done-thread/done-thread thread-data))]
      (Thread/sleep 100)
      (swap! runtime-config assoc :ramper/runtime-stop true)
      (is (true? (async/<!! thread)))
      (is (= 1 (count @refill-queue)))
      (is (= empty-entry (peek @refill-queue)))
      (is (= 3 (.numberOfEntries wb)))
      (is (= still-in-wb-entry (.popEntry wb))))))
