(ns ramper.workers.dns-resolving
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [lambdaisland.uri :as uri]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.frontier.workbench :as workbench]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]
            [ramper.util.delay-queue :as delay-queue]
            [ramper.util.persistent-queue :as pq])
  (:import (java.net InetAddress UnknownHostException)
           (org.xbill.DNS Address)))

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
        (into-array [(InetAddress/getByAddress hostname (byte-array address))])
        (cond
          (= "localhost" hostname) loopback
          (re-matches dotted-address hostname) (InetAddress/getAllByName hostname)
          :else (let [hostname (if (str/ends-with? hostname ".") hostname (str hostname "."))]
                  (Address/getAllByName hostname))))))))

(def global-host-map (atom {}))

(def global-java-dns-resolver
  "A global instance of `org.apache.http.conn.DnsResolver` that tries to look up
  the ip from the `global-host-map` before delegating to dnsjava."
  (reify
    org.apache.http.conn.DnsResolver
    (^"[Ljava.net.InetAddress;" resolve [this ^String hostname]
     (if-let [address (get @global-host-map hostname)]
       (into-array [(InetAddress/getByAddress hostname (byte-array address))])
       (cond
         (= "localhost" hostname) loopback
         (re-matches dotted-address hostname) (InetAddress/getAllByName hostname)
         :else (let [hostname (if (str/ends-with? hostname ".") hostname (str hostname "."))]
                 (Address/getAllByName hostname)))))))

(defn dns-thread [{:keys [dns-resolver workbench unknown-hosts
                          new-visit-states] :as _thread-data}
                  index stop-chan]
  (thread-utils/set-thread-name (str *ns* "-" index))
  (try
    (loop []
      (when-not (async/poll! stop-chan)
        ;; maybe add a timeout somewhere as this otherwise might put
        ;; too much pressure on new-visit-states
        (when-let [{:keys [retries] :as visit-state} (or (delay-queue/dequeue! unknown-hosts)
                                                         (pq/dequeue! new-visit-states))]
          (let [host (-> visit-state :scheme+authority uri/uri :host)]
            (try
              (let [ip-address (-> (.resolve dns-resolver host) first .getAddress)]
                (swap! workbench workbench/add-visit-state (-> visit-state
                                                               (assoc :ip-address ip-address)
                                                               (assoc :last-exception nil))))
              (catch UnknownHostException _e
                (log/warn :unknown-host-ex {:host host
                                            :visit-state visit-state})
                (let [{:keys [retries] :as visit-state}
                      (-> visit-state
                          (assoc :retries
                                 (if (= (:last-exception visit-state) UnknownHostException)
                                   (inc retries)
                                   0))
                          (assoc :last-exception UnknownHostException))]
                  (when (< retries (get constants/exception-to-max-retries UnknownHostException))
                    (let [delay (bit-shift-left (get constants/exception-to-wait-time UnknownHostException) retries)
                          next-fetch (util/from-now delay)
                          visit-state (assoc visit-state :next-fetch next-fetch)]
                      (log/info :retry-dns-resolution {:delay delay :visit-state visit-state})
                      (swap! unknown-hosts conj [visit-state next-fetch])))
                  ;; o/w the visit-state gets purged by garbage collection
                  ))))
          (recur))))
    (catch Throwable t
      (log/error :unexpected-ex (Throwable->map t))))
  (log/info :graceful-shutdown {:index index})
  true)
