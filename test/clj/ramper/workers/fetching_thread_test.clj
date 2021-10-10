(ns ramper.workers.fetching-thread-test
  (:require [clj-http.conn-mgr :as conn]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest is testing]]
            [matcher-combinators.test]
            [ramper.frontier.workbench2 :as workbench]
            [ramper.util :as util]
            [ramper.util.thread :as thread-util]
            [ramper.util.url :as url]
            [ramper.workers.dns-resolving :as dns-resolving]
            [ramper.workers.fetched-data :as fetched-data]
            [ramper.workers.fetching-thread :as fetching-thread])
  (:import (org.xbill.DNS Address)))

(deftest fetch-data-test
  (let [ip-address (Address/getByName "httpbin.org")
        runtime-config (atom {:ramper/scheme+authority-delay 2000})
        dns-resolver (dns-resolving/global-java-dns-resolver)
        cookie-store (cookies/cookie-store)
        connection-manager (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        client (core/build-http-client {:http-builder-fns [fetching-thread/set-connection-reuse]}
                                       false connection-manager)
        entry (-> (workbench/entry (url/scheme+authority "https://httpbin.org") ["/cookies" "/something/else"])
                  (assoc :ip-address ip-address
                         :last-exception Exception))
        entry-400-resp (-> (workbench/entry (url/scheme+authority "https://httpbin.org") ["/status/400" "/something/else"])
                           (assoc :ip-address ip-address))
        bad-entry (-> (workbench/entry (url/scheme+authority "https://asdf.asdf") ["/foo/bar" "/something/else"]))
        fetch-thread-data {:http-client client :dns-resolver dns-resolver
                           :cookie-store cookie-store :runtime-config runtime-config}]
    (testing "simple request with cookies"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry entry))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception entry)))
        (is (= "https://httpbin.org/something/else" (workbench/first-url entry)))
        (is (< now (:next-fetch entry)))))
    (testing "simple request with expected 400 response"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry entry-400-resp))]
        (is (not (nil? fetched-data)) "fetched-data is nil")
        (is (s/valid? ::fetched-data/fetched-data fetched-data) "fetched data does not conform to spec")
        (is (true? continue))
        (is (nil? (:last-exception entry )))
        (is (= "https://httpbin.org/something/else" (workbench/first-url entry)))
        (is (< now (:next-fetch entry)))))
    (testing "simple request with expected error"
      (let [now (System/currentTimeMillis)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry bad-entry))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (true? continue))
        (is (= java.net.UnknownHostException (:last-exception entry)))
        (is (= "https://asdf.asdf/foo/bar" (workbench/first-url entry)))
        (is (< now (:next-fetch entry)))))
    (testing "simple request with expected error that purges the entry"
      (let [bad-entry (assoc bad-entry :last-exception java.net.UnknownHostException :retries 1)
            [fetched-data entry continue]
            (fetching-thread/fetch-data (assoc fetch-thread-data :entry bad-entry))]
        (is (nil? fetched-data) "fetched-data is not nil")
        (is (false? continue))
        (is (= 2 (:retries entry)))
        (is (= java.net.UnknownHostException (:last-exception entry)))))))

(deftest fetching-thread-test
  (let [runtime-config (atom {:ramper/keepalive-time 5000 ;; this test should not depend on keep alive time
                              :ramper/scheme+authority-delay 2000
                              :ramper/ip-delay 2000
                              :ramper/required-front-size 1000})
        host1 "clojure.org"
        host2 "httpbin.org"
        host3 "finnvolkel.com" ;; explicitly using a wrong ip
        host1-ip (Address/getByName host1)
        host2-ip (Address/getByName host2)
        host3-ip (Address/getByName host3)
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
                     :done-queue done-queue
                     :stats-chan (async/chan (async/sliding-buffer 100))}
        tw (thread-util/thread-wrapper (partial fetching-thread/fetching-thread thread-data 1))]
    (Thread/sleep 2000)
    (thread-util/stop tw)
    (is (true? (thread-util/stopped? tw)))
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

(deftest fetching-thread-front-expansion
  (let [runtime-config (atom {:ramper/keepalive-time 5000 ;; this test should not depend on keep alive time
                              :ramper/scheme+authority-delay 2000
                              :ramper/ip-delay 2000
                              :ramper/required-front-size 1000})
        dns-resolver (dns-resolving/global-java-dns-resolver)
        conn-mgr (conn/make-reusable-conn-manager {:dns-resolver dns-resolver})
        workbench (atom (workbench/workbench))
        results-queue (atom clojure.lang.PersistentQueue/EMPTY)
        todo-queue (atom clojure.lang.PersistentQueue/EMPTY)
        done-queue (atom clojure.lang.PersistentQueue/EMPTY)
        thread-data {:connection-manager conn-mgr :dns-resolver dns-resolver
                     :workbench workbench :results-queue results-queue
                     :runtime-config runtime-config :todo-queue todo-queue
                     :done-queue done-queue
                     :stats-chan (async/chan (async/sliding-buffer 100))}
        tw (thread-util/thread-wrapper (partial fetching-thread/fetching-thread thread-data 1))]
    (Thread/sleep 500)
    (thread-util/stop tw)
    (is (true? (thread-util/stopped? tw)))
    (is (= (+ 1000 fetching-thread/front-increase) (:ramper/required-front-size @runtime-config)))))
