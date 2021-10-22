(ns ramper.workers.dns-resolving
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.ip-store :as ip-store]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.persistent-queue :as pq])
  (:import (java.net InetAddress UnknownHostException)
           (org.apache.http.conn DnsResolver)
           (org.xbill.DNS Address)
           (ramper.frontier Entry Workbench3)))

(def loopback (InetAddress/getByAddress (byte-array '(127 0 0 1))))

;; COPIED FROM BUbiNG

;; A pattern used to identify hosts specified directed via their address in dotted notation. Note the dot at the end.
;; It covers both IPv6 addresses (where hexadecimal notation is accepted by default) and IPv4 addresses (where hexadecimal notation
;; requires the 0x prefix on every single piece of the address).
(def dotted-address #"(([0-9A-Fa-f]+[:])*[0-9A-Fa-f]+)|((((0x[0-9A-Fa-f]+)|([0-9]+))\\.)*((0x[0-9A-Fa-f]+)|([0-9]+)))")

(defn java-dns-resolver
  "Creates an instance of `org.apache.http.conn.DnsResolver` with an optional
  static `host-map`."
  ([] (java-dns-resolver {}))
  ([host-map]
   (reify
     org.apache.http.conn.DnsResolver
     (^"[Ljava.net.InetAddress;" resolve [this ^String hostname]
      (if-let [address (get host-map hostname)]
        (into-array [address])
        (cond
          (= "localhost" hostname) (into-array [loopback])
          (re-matches dotted-address hostname) (InetAddress/getAllByName hostname)
          :else (let [hostname (if (str/ends-with? hostname ".") hostname (str hostname "."))]
                  (Address/getAllByName hostname))))))))

(defprotocol HostToIpAddress
  (addHost [this host ip-address])
  (deleteHost [this host]))

(defn global-java-dns-resolver
  "Creates an instance of `org.apache.http.conn.DnsResolver` that tries to look up
  the ip from the `global-host-map` before delegating to dnsjava."
  ([] (global-java-dns-resolver {}))
  ([host-map]
   (let [global-host-map (atom host-map)]
     (reify
       org.apache.http.conn.DnsResolver
       (^"[Ljava.net.InetAddress;" resolve [this ^String hostname]
        (if-let [address (get @global-host-map hostname)]
          (into-array [address])
          (cond
            (= "localhost" hostname) (into-array [loopback])
            (re-matches dotted-address hostname) (InetAddress/getAllByName hostname)
            :else (let [hostname (if (str/ends-with? hostname ".") hostname (str hostname "."))]
                    (Address/getAllByName hostname)))))

       HostToIpAddress
       (addHost [_ host ip-address]
         (swap! global-host-map assoc host ip-address))

       (deleteHost [_ host]
         (swap! global-host-map dissoc host))))))

(def ^:private the-ns-name (str *ns*))

(defn get-ip-address [host ip-store-wrapped ^DnsResolver dns-resolver]
  (if (contains? @ip-store-wrapped host)
    (do
      (swap! ip-store-wrapped ip-store/ping host)
      (ip-store/get @ip-store-wrapped host))
    (let [ip-address (first (.resolve dns-resolver host))]
      (swap! ip-store-wrapped ip-store/add host ip-address)
      ip-address)))

(defn dns-thread [{:keys [dns-resolver workbench unknown-hosts
                          new-entries ip-store] :as _thread-data}
                  index stop-chan]
  (thread-utils/set-thread-name (str the-ns-name "-" index))
  (try
    (loop [i 0]
      (when-not (async/poll! stop-chan)
        ;; maybe add a timeout somewhere as this otherwise might put
        ;; too much pressure on new-entries
        (if-let [^Entry entry (or (delay-queue/dequeue! unknown-hosts) (pq/dequeue! new-entries))]
          (do
            (log/trace :dns-thread {:host (-> (.-schemeAuthority entry) uri/uri :host)})
            (if-let [host (-> (.-schemeAuthority entry) uri/uri :host)]
              (try
                ;; TODO should we maybe store InetAddress4 format?
                (let [ip-address (get-ip-address host ip-store dns-resolver)]
                  (.addEntry ^Workbench3 workbench
                             (doto entry
                               (.setIpAddress ip-address)
                               (.setLastException nil))))
                (catch UnknownHostException _e
                  (log/warn :unknown-host-ex {:host host
                                              :visit-state entry})
                  (let [last-exception (.getLastException entry)
                        retries (.getRetries entry)
                        entry (doto entry
                                (.setRetries (if (= last-exception UnknownHostException) (inc retries) 0))
                                (.setLastException UnknownHostException))]
                    (when (< retries (constants/get-exception-to-max-retries UnknownHostException))
                      (let [delay (bit-shift-left (constants/get-exception-to-wait-time UnknownHostException) retries)
                            next-fetch (util/from-now delay)
                            entry (doto entry (.setNextFetch next-fetch))]
                        (log/info :retry-dns-resolution {:delay delay :visit-state entry})
                        (swap! unknown-hosts conj [entry next-fetch])))
                    ;; o/w the visit-state gets purged by garbage collection
                    )))
              (log/warn :scheme+authority-no-host {:scheme+authority (.-schemeAuthority entry)}))
            (recur 0))
          (let [time (bit-shift-left 1 (max 10 i))
                timeout-chan (async/timeout time)]
            (log/info :dns-thread {:sleep-time time
                                   :index index})
            (when (= :timeout (async/alt!! timeout-chan :timeout stop-chan :stop))
              (recur (inc i)))))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :graceful-shutdown {:type :dns-thread
                                :index index})
  true)
