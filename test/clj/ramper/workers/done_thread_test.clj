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
  (:import (java.net InetAddress)))

(defn- create-dummy-ip [s]
  (let [ba (byte-array 4)]
    (doall (map-indexed #(aset-byte ba %1 %2) s))
    (InetAddress/getByAddress ba)))

(deftest done-thread-test
  (testing "done-thread"
    (let [dummy-ip1 (create-dummy-ip [1 2 3 4])
          dummy-ip2 (create-dummy-ip [2 3 4 5])
          dummy-ip3 (create-dummy-ip [3 4 5 6])
          url1 (url/scheme+authority "https://finnvolkel.com")
          url2 (url/scheme+authority "https://finnvolkel2.com")
          url3 (url/scheme+authority "https://clojure.org")
          url4 (url/scheme+authority "https://news.ycombinator.com")
          not-empty-vs (-> (visit-state/visit-state url1)
                           (assoc :next-fetch (util/from-now 40)
                                  :ip-address dummy-ip1
                                  :locked-entry true)
                           (visit-state/enqueue-path-query "/foo/bar")
                           (visit-state/enqueue-path-query "/foo/bla"))
          vs-still-in-wb (-> (visit-state/visit-state url2)
                             (assoc :next-fetch (util/from-now 10)
                                    :ip-address dummy-ip1)
                             (visit-state/enqueue-path-query "/foo/bar")
                             (visit-state/enqueue-path-query "/foo/bla"))
          empty-vs (-> (visit-state/visit-state url3)
                       (assoc :next-fetch (util/from-now 30)
                              :ip-address dummy-ip2))
          purgeable-visit-state (-> (visit-state/visit-state url4)
                                    (assoc :next-fetch (util/from-now 20)
                                           :ip-address dummy-ip3))
          non-empty-we (-> (we/workbench-entry dummy-ip1)
                           (we/add vs-still-in-wb))
          wb (atom (-> (workbench/workbench)
                       (update :address-to-busy-entry assoc (workbench/hash-ip dummy-ip1) non-empty-we)
                       (update :address-to-busy-entry assoc (workbench/hash-ip dummy-ip2) (we/workbench-entry dummy-ip2))
                       (update :address-to-busy-entry assoc (workbench/hash-ip dummy-ip3) (we/workbench-entry dummy-ip3))
                       (update :scheme+authorities #(into % [url1 url2 url3 url4]))))
          virtualizer (virtual/workbench-virtualizer (util/temp-dir "done-thread-test"))
          _ (do (virtual/enqueue virtualizer empty-vs "/foo/bar")
                (virtual/enqueue virtualizer empty-vs "/foo/bla"))
          runtime-config (atom {:ramper/runtime-stop false})
          done-queue (atom (into clojure.lang.PersistentQueue/EMPTY [not-empty-vs empty-vs purgeable-visit-state]))
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
      (is (= empty-vs (peek @refill-queue)))
      (is (= 3 (-> wb deref :scheme+authorities count)))
      (is (= 2 (workbench/nb-workbench-entries @wb)))
      (is (= vs-still-in-wb (dissoc (workbench/peek-visit-state @wb) :locked-entry))))))
