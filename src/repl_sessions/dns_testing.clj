(ns repl-sessions.dns-testing
  (:require [clj-http.client :as client]
            [clj-http.conn-mgr :as conn]
            [clj-http.core :as core]
            [clojure.string :as str])
  (:import (java.net InetAddress Inet4Address Inet6Address)
           (org.xbill.DNS Address)))


(def addr (InetAddress/getAllByName "www.finnvolkel.com"))

(InetAddress/getByAddress )

(-> addr first .getHostAddress)

(def addr (Address/getAllByName "www.finnvolkel.com"))


(let [cm (conn/make-reusable-conn-manager {})
      hclient (core/build-http-client {} false cm)]
  (client/get "https://finnvolkel.com" {:connection-manager cm :http-client hclient}))

(def loopback (InetAddress/getByAddress (byte-array '(127 0 0 1))))

;; COPIED FROM BUbiNG

;; A pattern used to identify hosts specified directed via their address in dotted notation. Note the dot at the end.
;; It covers both IPv6 addresses (where hexadecimal notation is accepted by default) and IPv4 addresses (where hexadecimal notation
;; requires the 0x prefix on every single piece of the address).
(def dotted-address #"(([0-9A-Fa-f]+[:])*[0-9A-Fa-f]+)|((((0x[0-9A-Fa-f]+)|([0-9]+))\\.)*((0x[0-9A-Fa-f]+)|([0-9]+)))")

(defn java-dns-resolver
  ([] (java-dns-resolver {}))
  ([host-map]
   (reify
     org.apache.http.conn.DnsResolver
     (^"[Ljava.net.InetAddress;" resolve [this ^String hostname]
      (if-let [address (get host-map hostname)]
        (into-array [(java.net.InetAddress/getByAddress hostname (byte-array address))])
        (cond
          (= "localhost" hostname) loopback
          (re-matches dotted-address hostname) (InetAddress/getAllByName hostname)
          :else (let [hostname (if (str/ends-with? hostname ".") hostname (str hostname "."))]
                  (Address/getAllByName hostname))))))))


(def dns-resolver (java-dns-resolver))

(let [cm (conn/make-reusable-conn-manager {})
      hclient (core/build-http-client {} false cm)]
  (client/get "https://finnvolkel.com/about"
              {:connection-manager cm
               :http-client hclient
               :dns-resolver dns-resolver}))
