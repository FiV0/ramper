(ns ramper.workers.fetching-thread
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clj-http.core :as core]
            [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.constants :as constants]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.persistent-queue :as pq]
            [ramper.util.thread :as thread-utils]
            [ramper.workers.fetched-data :as fetched-data])
  (:import (java.io IOException)
           (org.apache.http.cookie Cookie)
           (org.apache.http.impl DefaultConnectionReuseStrategy)
           (org.apache.http.impl.client BasicCookieStore HttpClientBuilder)
           (ramper.frontier Entry Workbench3)
           (ramper.workers.dns_resolving HostToIpAddress)))

;; TODO make configurable
(def front-increase 100)

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

(defn fetch-data
  "Builds a url from the visit-state and dequeues the first path-query in
  case of success. It serves as a helper function for a fetching thread.

  The given fetch-thread-data map must contain.

  :http-client - an http client with which to do the request.

  :dns-resolver - a dns resolver implementing org.apache.http.conn.DnsResolver
  and ramper.workers.dns-resolving.HostToIpAddress. The above http-client
  must be intialized with this dns resolver.

  :visit-state - the visit state from which to build the queried url

  :runtime-config - an atom wrapping the runtime config of the agent

  :cookie-store - the cookie store of the http-client"
  [{:keys [http-client dns-resolver ^Entry entry runtime-config cookie-store] :as _fetch-thread-data}]
  (let [scheme+authority (.-schemeAuthority entry)
        url (str scheme+authority (.peekPathQuery entry))
        scheme+authority-delay (:ramper/scheme+authority-delay @runtime-config)]
    (if-let [ip-address (.getIpAddress entry)]
      (.addHost ^HostToIpAddress dns-resolver (:host (uri/uri scheme+authority)) ip-address)
      (log/warn :missing-ip-address {:entry entry}))
    (try
      ;; TODO maybe improve this with respect to early termination/timeouts
      (let [resp (client/get url {:http-client http-client
                                  :headers default-headers
                                  :cookie-store cookie-store
                                  :throw-exceptions false
                                  ;; TODO make configurable
                                  :connection-timeout 2000
                                  :socket-timeout 2000})
            now (System/currentTimeMillis)
            fetched-data {:url (uri/uri url) :response resp}
            entry (doto entry
                    (.setNextFetch (+ now scheme+authority-delay))
                    (.setLastException nil)
                    (.popPathQuery))]
        [fetched-data entry true])
      ;; normal exception case
      (catch IOException ex
        (let [exception-type (type ex)
              now (System/currentTimeMillis)
              _ (cond
                  (nil? (.getLastException entry))
                  (.setLastException entry exception-type)

                  (= (.getLastException entry) exception-type)
                  (.setRetries entry (inc (.getRetries entry)))

                  :else
                  (.setLastException entry exception-type))]
          (log/warn :fetch-ex {:url url :ex exception-type})
          (cond
            ;; normal retry case
            (< (.getRetries entry) (constants/get-exception-to-max-retries exception-type))
            (do
              (.setNextFetch entry (+ now scheme+authority-delay))
              (log/info :url-retry {:url url :ex exception-type :retries (.getRetries entry)})
              [nil entry true])

            ;; purge case
            (contains? constants/exception-host-killer exception-type)
            (do
              (log/warn :visit-state-purge {:url url :ex exception-type})
              [nil entry false])

            ;; else just dequeue and continue
            :else
            (do
              (log/info :url-killed {:url url :ex exception-type})
              [nil (doto entry
                     (.setNextFetch (+ now scheme+authority-delay))
                     (.setLastException nil)
                     (.popPathQuery))
               true]))))
      ;; bad exception case
      (catch Exception should-not-happen
        (log/error :unexpected-ex {:url url :ex (type should-not-happen)})
        ;; we return the visit-state as is, but with no exception set
        ;; this happens with invalid certificates
        (let [now (System/currentTimeMillis)]
          [nil (doto entry
                 (.setNextFetch (+ now scheme+authority-delay))
                 (.popPathQuery))
           true]))
      (finally
        (.deleteHost ^HostToIpAddress dns-resolver (:host scheme+authority))))))

(defn- estimate-cookie-size
  "Returns an approximate size of the `cookie` in bytes."
  [^Cookie cookie]
  (->> [(.getName cookie) (.getValue cookie) (.getDomain cookie) (.getPath cookie)]
       (map count)
       (apply +)))

(defn limit-cookies
  "Returns a sequence of `cookies` limited to the overall size `cookies-max-byte-size`."
  [cookies cookies-max-byte-size]
  (loop [[cookie & cookies] cookies res [] total-size 0]
    (if (nil? cookie) res
        (let [cookie-size (estimate-cookie-size cookie)
              total-size (+ total-size cookie-size)]
          (if (< total-size cookies-max-byte-size)
            (recur cookies (conj res cookie) total-size)
            res)))))

(def ^:private the-ns-name (str *ns*))

(defn fetching-thread
  "Continuously tries to take visit-states from a todo-queue and tries to fetch as many
  resources as allowed in the keep-alive time of the runtime config. Pushes fetched
  data to a results-queue and reusable visit states to the done queue. Visit states
  might get purged from the workbench if they trigger too many errors.

  The 3 arguments:
  - a `thread-data` map (see more below)
  - a `index` integer - identifying the fetching thread
  - a `stop-chan` as this function follows the thread-wrapper pattern (see also
  ramper.util.thread/thread-wrapper)

  The thread-data map must contain:

  :connection-manager - a http connection manager from which to build a http client.

  :dns-resolver - a dns resolver with which also the above connection manager was
  initialized.

  :results-queue - an atom wrapping a clojure.lang.PersistentQueue to which fetched data
  can be pushed.

  :workbench - an atom wrapping the agents workbench.

  :runtime-config - an atom wrapping the runtime config of the agent.

  :todo-queue - an atom wrapping a clojure.lang.PersistentQueue from which ready visit
  states can be dequeued.

  :done-queue - an atom wrapping a clojure.lang.PersistentQueue to which finished visit
  states can be enqueued."
  [{:keys [connection-manager results-queue ^Workbench3 workbench
           runtime-config todo-queue done-queue stats-chan] :as thread-data}
   index stop-chan]
  (thread-utils/set-thread-name (str the-ns-name "-" index))
  (thread-utils/set-thread-priority Thread/MIN_PRIORITY)
  ;; TODO check if cookie store should be added via HttpClientBuilder
  (try
    (let [^BasicCookieStore cookie-store (cookies/cookie-store)
          ip-delay (:ramper/ip-delay @runtime-config)
          http-client (core/build-http-client {:http-builder-fns [set-connection-reuse]}
                                              false connection-manager)
          cookies-max-byte-size (:ramper/cookies-max-byte-size @runtime-config)]

      (loop [i 0 wait-time 0]
        (when-not (async/poll! stop-chan)
          (if-let [entry (pq/dequeue! todo-queue)]
            (let [start-time (System/currentTimeMillis)]
              (loop [^Entry e entry]
                (if (and (not (.isEmpty e))
                         (<= (- start-time (System/currentTimeMillis))
                             (:ramper/keepalive-time @runtime-config))
                         ;; TODO do this more elegently
                         (not (runtime-config/stop? @runtime-config)))
                  ;; TODO does the cookie unrolling and readding need to happen here all the time?
                  (do
                    (cookies/clear-cookies cookie-store)
                    (run! #(cookies/add-cookie cookie-store %) (.getCookies e))
                    (let [[fetched-data e continue] (fetch-data (assoc thread-data
                                                                       :http-client http-client
                                                                       :entry e
                                                                       :cookie-store cookie-store))
                          now (System/currentTimeMillis)]
                      (cond
                        ;; no error case
                        fetched-data
                        (let [cookies (->> (limit-cookies (seq (.getCookies cookie-store)) cookies-max-byte-size)
                                           (into-array org.apache.http.cookie.Cookie))]
                          (s/assert ::fetched-data/fetched-data fetched-data)
                          (swap! results-queue conj fetched-data)
                          (when (seq cookies) (.setCookies e (to-array cookies)))
                          (recur e))
                        ;; an error occurred, but the visit-state does not need to be purged
                        continue
                        (swap! done-queue conj (doto e (.setNextFetch (+ now ip-delay))))
                        ;; we are purging the visit state
                        :else
                        (do
                          ;; TODO remove once optimized
                          (async/offer! stats-chan {:fetching-thread/purge 1})
                          (.purgeEntry workbench entry)))))
                  (swap! done-queue conj e)))
              (recur 0 wait-time))

            (let [time (bit-shift-left 1 (max 10 i))
                  timeout-chan (async/timeout time)
                  ;; TODO figure out if this is not excessive
                  increase-front (compare-and-set! runtime-config @runtime-config
                                                   (update @runtime-config :ramper/required-front-size + front-increase))]
              (async/offer! stats-chan {:fetching-thread/sleep time})
              (log/trace :fetching-thread
                         (cond-> {:sleep-time time
                                  :index index}
                           increase-front
                           (assoc :front-increase front-increase)))
              (when (= :timeout (async/alt!! timeout-chan :timeout stop-chan :stop))
                (recur (inc i) (+ wait-time time))))))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :graceful-shutdown {:type :fetching-thread
                                :index index})
  true)
