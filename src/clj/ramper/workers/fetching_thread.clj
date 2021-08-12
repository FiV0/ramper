(ns ramper.workers.fetching-thread
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.constants :as constants]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.persistent-queue :as pq]
            [ramper.util.thread :as thread-utils]
            [ramper.workers.fetched-data :as fetched-data])
  (:import (java.io IOException)
           (org.apache.http.impl DefaultConnectionReuseStrategy)
           (org.apache.http.impl.client HttpClientBuilder)))

;; results data is of the form
;; {:url ... :response ...}

;; TODO can the cookie store be set similarly
(defn set-connection-reuse [^HttpClientBuilder builder req]
  (when (:connection-reuse req)
    (.setConnectionReuseStrategy builder DefaultConnectionReuseStrategy/INSTANCE)))

;; TODO add default-headers in client buildup see (.setDefaultHeaders builder)
(def default-headers {:from (:ramper/user-agent @runtime-config/runtime-config)
                      :accept "text/html,application/xhtml+xml,application/xml;q=0.95,text/*;q=0.9,*/*;q=0.8"})

;; The function(s) below needs quite a bit of updates for a final version
;; TODO update request front size if wait-time > 0
;; TODO send statistics of wait-time
;; TODO RobotsPath checking
;; TODO RobotsPath filtering
;; TODO fetch-filter function for urls
;; TODO blacklisted hosts
;; TODO blacklisted ips
;; TODO cookie size limit

(defn fetch-data [{:keys [http-client dns-resolver visit-state runtime-config cookie-store] :as _fetch-thread-data}]
  (let [scheme+authority (str (:scheme+authority visit-state))
        url (str (:scheme+authority visit-state) (visit-state/first-path visit-state))
        scheme+authority-delay (:ramper/scheme+authority-delay @runtime-config)]
    (.addHost dns-resolver (:host scheme+authority) (:ip-address visit-state))
    (try
      (let [resp (client/get url {:http-client http-client
                                  :headers default-headers
                                  :cookie-store cookie-store
                                  :throw-exceptions false})
            now (System/currentTimeMillis)
            fetched-data {:url (uri/uri url) :response resp}
            visit-state (-> visit-state
                            visit-state/dequeue-path-query
                            (assoc :next-fetch (+ now scheme+authority-delay)
                                   :last-exception nil))]
        [fetched-data visit-state true])
      ;; normal exception case
      (catch IOException ex
        (let [exception-type (type ex)
              now (System/currentTimeMillis)
              visit-state (cond
                            (nil? (:last-exception visit-state))
                            (assoc visit-state :last-exception exception-type :retries 0)

                            (= (:last-exception visit-state) exception-type)
                            (update visit-state :retries inc)

                            :else
                            (assoc visit-state :last-exception exception-type))]
          (log/warn :fetch-ex {:url url :ex exception-type})
          (cond
            ;; normal retry case
            (< (:retries visit-state) (constants/get-exception-to-max-retries exception-type))
            (let [visit-state (assoc visit-state :next-fetch (+ now scheme+authority-delay))]
              (log/info :url-retry {:url url :ex exception-type :retries (:retries visit-state)})
              [nil visit-state true])

            ;; purge case
            (contains? constants/exception-host-killer exception-type)
            (do
              (log/warn :visit-state-purge {:url url :ex exception-type})
              [nil visit-state false])

            ;; else just dequeue and continue
            :else
            (let [visit-state (-> visit-state
                                  visit-state/dequeue-path-query
                                  (assoc :next-fetch (+ now scheme+authority-delay)
                                         :last-exception nil))]
              (log/info :url-killed {:url url :ex exception-type})
              [nil visit-state true]))))
      ;; bad exception case
      (catch Exception should-not-happen
        (log/error :unexpected-ex (Throwable->map should-not-happen))
        ;; we return the visit-state as is, but with no exception set
        ;; this really should not happen
        (let [now (System/currentTimeMillis)
              visit-state (-> visit-state
                              visit-state/dequeue-path-query
                              (assoc :next-fetch (+ now scheme+authority-delay)))]
          [nil visit-state true]))
      (finally
        (.deleteHost dns-resolver (:host scheme+authority))))))


(defn fetching-thread [{:keys [connection-manager _dns-resolver results-queue workbench
                               runtime-config todo-queue done-queue] :as thread-data}
                       index stop-chan]
  (thread-utils/set-thread-name (str *ns* "-" index))
  (thread-utils/set-thread-priority Thread/MIN_PRIORITY)
  ;; TODO check if cookie store should be added via HttpClientBuilder
  (let [cookie-store (cookies/cookie-store)
        ip-delay (:ramper/ip-delay @runtime-config)
        http-client (core/build-http-client {:http-builder-fns [set-connection-reuse]}
                                            false connection-manager)
        ;; cookie-max-byte-size (:ramper/cookie-max-byte-size @runtime-config)
        ]
    (try
      (loop [i 0 wait-time 0]
        (when-not (async/poll! stop-chan)
          (if-let [visit-state (pq/dequeue! todo-queue)]
            (let [start-time (System/currentTimeMillis)]
              (loop [vs visit-state]
                (if (and (visit-state/first-path vs)
                         (<= (- start-time (System/currentTimeMillis))
                             (:ramper/keepalive-time @runtime-config)))
                  ;; TODO does the cooking unrolling and readding need to happen here all the time
                  (do
                    (cookies/clear-cookies cookie-store)
                    (run! #(cookies/add-cookie cookie-store %) (:cookies vs))
                    (let [[fetched-data vs continue] (fetch-data (assoc thread-data
                                                                        :http-client http-client
                                                                        :visit-state vs
                                                                        :cookie-store cookie-store))
                          now (System/currentTimeMillis)]
                      (cond
                        ;; no error case
                        fetched-data
                        (do
                          (s/assert ::fetched-data/fetched-data fetched-data)
                          (swap! results-queue conj fetch-data)
                          (recur (visit-state/set-cookies vs (seq (.getCookies cookie-store)))))
                        ;; an error occurred but the visit-state does not need to be purged
                        continue
                        (do
                          (swap! workbench workbench/set-entry-next-fetch (:ip-address visit-state) (+ now ip-delay))
                          (swap! done-queue conj vs))
                        ;; we are purging the visit state
                        :else
                        (do
                          (swap! workbench workbench/purge-visit-state vs)
                          (swap! workbench workbench/set-entry-next-fetch (:ip-address visit-state) (+ now ip-delay))))))
                  (swap! done-queue conj vs))))
            (let [time (bit-shift-left 1 (max 10 i))]
              (Thread/sleep time)
              (recur (inc i) (+ wait-time time))))))
      (catch Throwable t
        (log/error :unexpected-ex (Throwable->map t)))))
  (log/info :fetching-thread :graceful-shutdown)
  true)
