(ns ramper.workers.fetching-thread-test
  (:require [clj-http.conn-mgr :as conn]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [matcher-combinators.test]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.util :as util]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving]
            [ramper.workers.fetched-data :as fetched-data]
            [ramper.workers.fetching-thread :as fetching-thread])
  (:import (org.xbill.DNS Address)))

(deftest fetch-data-test
  (let [ip-address (.getAddress (Address/getByName "httpbin.org"))
        runtime-config (atom {:ramper/scheme+authority-delay 2000})
        dns-resolver (dns-resolving/global-java-dns-resolver)
        cookie-store (cookies/cookie-store)
        connection-manager (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        client (core/build-http-client {:http-builder-fns [fetching-thread/set-connection-reuse]}
                                       false connection-manager)
        visit-state (-> (visit-state/visit-state (url/scheme+authority "https://httpbin.org"))
                        (assoc :ip-address ip-address
                               :last-exception Exception)
                        (visit-state/enqueue-path-query "/cookies")
                        (visit-state/enqueue-path-query "/something/else"))
        visit-state-400-resp (-> (visit-state/visit-state (url/scheme+authority "https://httpbin.org"))
                                 (assoc :ip-address ip-address)
                                 (visit-state/enqueue-path-query "/status/400")
                                 (visit-state/enqueue-path-query "/something/else"))
        bad-visit-state (-> (visit-state/visit-state (url/scheme+authority "https://asdf.asdf"))
                            (visit-state/enqueue-path-query "/foo/bar")
                            (visit-state/enqueue-path-query "/something/else"))
        fetch-thread-data {:http-client client :dns-resolver dns-resolver
                           :cookie-store cookie-store :runtime-config runtime-config}]
    (testing "simple request with cookies"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state visit-state))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception visit-state)))
        (is (= "/something/else" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected 400 response"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state visit-state-400-resp))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception visit-state)))
        (is (= "/something/else" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected error"
      (let [now (System/currentTimeMillis)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state bad-visit-state))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (true? continue))
        (is (= java.net.UnknownHostException (:last-exception visit-state)))
        (is (= "/foo/bar" (visit-state/first-path visit-state)))
        (is (< now (:next-fetch visit-state)))))
    (testing "simple request with expected error that purges the visit-state"
      (let [bad-visit-state (assoc bad-visit-state :last-exception java.net.UnknownHostException :retries 1)
            [fetched-data visit-state continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :visit-state bad-visit-state))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (false? continue))
        (is (= 2 (:retries visit-state)))
        (is (= java.net.UnknownHostException (:last-exception visit-state)))))))

(deftest fetching-thread-test
  (let [runtime-config (atom {:ramper/keepalive-time 5000 ;; this test should not depend on keep alive time
                              :ramper/scheme+authority-delay 2000
                              :ramper/ip-delay 2000})
        host1 "clojure.org"
        host2 "httpbin.org"
        host3 "finnvolkel.com" ;; explicitly using a wrong ip
        host1-ip (.getAddress (Address/getByName host1))
        host2-ip (.getAddress (Address/getByName host2))
        host3-ip (.getAddress (Address/getByName host3))
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        visit-state (-> (visit-state/visit-state (url/scheme+authority "https://clojure.org"))
                        (assoc :ip-address host1-ip
                               :next-fetch (util/from-now -30))
                        (visit-state/enqueue-path-query "")
                        (visit-state/enqueue-path-query "/about/rationale"))
        visit-state-with-400-resp (-> (visit-state/visit-state (url/scheme+authority "https://httpbin.org"))
                                      (assoc :ip-address host2-ip
                                             :next-fetch (util/from-now -20))
                                      (visit-state/enqueue-path-query "/status/400")
                                      (visit-state/enqueue-path-query "/get"))
        ;; we use a wrong ip to force an certificate error
        bad-visit-state (-> (visit-state/visit-state (url/scheme+authority "https://asdf.asdf"))
                            (assoc :ip-address host3-ip
                                   :next-fetch (util/from-now -10))
                            (visit-state/enqueue-path-query "/foo/bar")
                            (visit-state/enqueue-path-query "/something/else"))
        ;; we let things go through the workbench so bookkeeping in the workbench is correct
        workbench (atom (reduce #(workbench/add-visit-state %1 %2 )
                                (workbench/workbench)
                                [visit-state visit-state-with-400-resp bad-visit-state]))
        results-queue (atom clojure.lang.PersistentQueue/EMPTY)
        todo-queue (atom (into clojure.lang.PersistentQueue/EMPTY
                               (repeatedly 3 #(workbench/dequeue-visit-state! workbench))))
        done-queue (atom clojure.lang.PersistentQueue/EMPTY)
        thread-data {:connection-manager conn-mgr :dns-resolver dns-resolver
                     :workbench workbench :results-queue results-queue
                     :runtime-config runtime-config :todo-queue todo-queue
                     :done-queue done-queue}
        tw (thread-util/thread-wrapper (partial fetching-thread/fetching-thread thread-data 1))]
    (Thread/sleep 2000)
    (is (true? (thread-util/stop tw)))
    (is (empty? @todo-queue))
    (is (= 2 (count @done-queue)))
    (is (match? (-> visit-state
                    (dissoc :next-fetch :path-queries :cookies))
                (peek @done-queue)))
    (is (match? (-> visit-state-with-400-resp
                    (dissoc :next-fetch :path-queries :cookies))
                (peek (pop @done-queue))))
    (is (= 4 (count @results-queue)))
    (is (every? #(s/valid? ::fetched-data/fetched-data %) @results-queue))
    (is (match? [200 200 400 200] (map #(-> % :response :status) @results-queue)))))
