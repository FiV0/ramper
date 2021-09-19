(ns ramper.workers.todo-thread-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.workers.todo-thread :as todo-thread]
            [ramper.util :as util]
            [ramper.util.url :as url])
  (:import (java.net InetAddress)))

(defn- create-dummy-ip [s]
  (let [ba (byte-array 4)]
    (doall (map-indexed #(aset-byte ba %1 %2) s))
    (InetAddress/getByAddress ba)))

(comment
  (util/InetAddress->str (create-dummy-ip [1 2 3 4])))

(deftest todo-thread-test
  (testing "todo-thread"
    (let [dummy-ip1 (create-dummy-ip [1 2 3 4])
          dummy-ip2 (create-dummy-ip [2 3 4 5])
          dummy-ip3 (create-dummy-ip [3 4 5 6])
          vs1 (-> (visit-state/visit-state (url/scheme+authority "https://finnvolkel.com"))
                  (assoc :next-fetch (util/from-now 10)
                         :ip-address dummy-ip1))
          vs2 (-> (visit-state/visit-state (url/scheme+authority "https://clojure.org"))
                  (assoc :next-fetch (util/from-now 30)
                         :ip-address dummy-ip2))
          vs3 (-> (visit-state/visit-state (url/scheme+authority "https://news.ycombinator.com"))
                  (assoc :next-fetch (util/from-now 20)
                         :ip-address dummy-ip3))
          wb (atom (reduce workbench/add-visit-state (workbench/workbench) [vs1 vs2 vs3]))
          runtime-config (atom {:ramper/runtime-stop false
                                :ramper/max-urls-per-scheme+authority 10})
          todo-queue (atom clojure.lang.PersistentQueue/EMPTY)
          scheme+authority-to-count (atom {(url/scheme+authority "https://finnvolkel.com") 5
                                           (url/scheme+authority "https://clojure.org") 3
                                           (url/scheme+authority "https://news.ycombinator.com") 2})
          thread-data {:workbench wb
                       :runtime-config runtime-config
                       :scheme+authority-to-count scheme+authority-to-count
                       :todo-queue todo-queue}
          thread (async/thread (todo-thread/todo-thread thread-data))]
      (Thread/sleep 100)
      (swap! runtime-config assoc :ramper/runtime-stop true)
      (is (true? (async/<!! thread)))
      (is (= 3 (count @todo-queue)))
      (is (= (list vs1 vs3 vs2) (map #(dissoc % :locked-entry) @todo-queue))))))
