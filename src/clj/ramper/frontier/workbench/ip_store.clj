(ns ramper.frontier.workbench.ip-store
  (:refer-clojure :exclude [remove])
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.runtime-configuration :as runtime-config])
  (:import (lambdaisland.uri URI)))

;; the ip-store is just a map from host to ip address
;; stored as java.net.InetAddress and a timestamp

(defn ip-store [] {})

(defn add [ip-store host ip-address]
  (assoc ip-store host [ip-address (System/currentTimeMillis)]))

(defn remove [ip-store host]
  (dissoc ip-store host))

(defn ping [ip-store host]
  (if (contains? ip-store host)
    (update ip-store host (fn [[ip-address _]] [ip-address (System/currentTimeMillis)]))
    ip-store))

(defn ip-store-loop [runtime-config {:keys [ip-store] :as _frontier}]
  (async/go
    (try
      (while (not (runtime-config/stop? @runtime-config))
        (let [now (System/currentTimeMillis)]
          (doseq [[host time] (vals @ip-store)]
            (when (> (- now constants/ip-purge-interval) time)
              (swap! ip-store remove host)))
          (async/<! (async/timeout (max 0 (- constants/ip-purge-interval (- (System/currentTimeMillis) now)))))))
      (catch Throwable t
        (log/error :unexpected-ex {:ex t})))
    (log/info :graceful-shutdown {:type :ip-store-loop})
    true))

(comment
  (def ip-store-atom (atom (ip-store)))

  (swap! ip-store-atom add "foo.bar" (byte-array 4))
  (deref ip-store-atom)
  (swap! ip-store-atom ping "foo.bar")
  (swap! ip-store-atom remove "foo/bar")

  (def runtime-config (atom {:ramper/runtime-stop false}))

  (def return-chan
    (binding [constants/ip-purge-interval 1000]
      (ip-store-loop runtime-config {:ip-store ip-store-atom})))

  (swap! runtime-config assoc :ramper/runtime-stop true)

  )
