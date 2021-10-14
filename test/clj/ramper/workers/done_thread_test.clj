(ns ramper.workers.done-thread-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench2 :as workbench]
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
          url1 (url/scheme+authority "https://finnvolkel.com")
          url2 (url/scheme+authority "https://finnvolkel2.com")
          url3 (url/scheme+authority "https://clojure.org")
          url4 (url/scheme+authority "https://asdf.asdf")
          not-empty-entry (-> (workbench/entry url1 ["/foo/bar" "/foo/bla"])
                              (assoc :next-fetch (util/from-now 40)
                                     :ip-address dummy-ip1))
          entry-still-in-wb (-> (workbench/entry url2 ["/foo/bar" "/foo/bla"])
                                (assoc :next-fetch (util/from-now 10)
                                       :ip-address dummy-ip1))
          empty-entry (-> (workbench/entry url3)
                          (assoc :next-fetch (util/from-now 30)
                                 :ip-address dummy-ip2))
          empty-with-paths-in-wb (-> (workbench/entry url4)
                                     (assoc :next-fetch (util/from-now 30)
                                            :ip-address dummy-ip2))
          wb (-> (workbench/workbench)
                 (workbench/add-entry entry-still-in-wb)
                 (workbench/add-path-query url4 "/hello"))
          wb (atom (reduce workbench/add-scheme+authority wb [url1 url2 url3 url4]))
          runtime-config (atom {:ramper/runtime-stop false})
          done-queue (atom (into clojure.lang.PersistentQueue/EMPTY [not-empty-entry empty-entry empty-with-paths-in-wb]))
          refill-queue (atom clojure.lang.PersistentQueue/EMPTY)
          thread-data {:workbench wb
                       :runtime-config runtime-config
                       :done-queue done-queue
                       :refill-queue refill-queue}
          thread (async/thread (done-thread/done-thread thread-data))]
      (Thread/sleep 100)
      (swap! runtime-config assoc :ramper/runtime-stop true)
      (is (true? (async/<!! thread)))
      (is (= 1 (count @refill-queue)))
      (is (= empty-entry (peek @refill-queue)))
      (is (= 4 (workbench/nb-workbench-entries @wb)))
      (is (= entry-still-in-wb (workbench/peek-entry @wb))))))
