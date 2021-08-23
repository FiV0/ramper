(ns ramper.runtime-configuration
  "A certain number of global (configurable) variables that the
  crawler agent has access to via the global accessible atom
  `runtime-config`. Some options come from ramper.startup-configuration,
  others are sensible defaults. Some can be modified via jmx methods
  at runtime."
  (:require [clojure.java.io :as io]
            [ramper.util :as util]))

(def ^:private root-dir (util/temp-dir "ramper-root"))

;; these are the default values when developing
(def runtime-config (atom {:ramper/aux-buffer-size               (* 64 1024)
                           :ramper/cookies-max-byte-size         2048
                           :ramper/dns-threads                   1
                           :ramper/fetching-threads              10
                           :ramper/frontier-dir                  (io/file root-dir "frontier")
                           :ramper/ip-delay                      2000 ;2 seconds
                           :ramper/is-new                        true
                           :ramper/keepalive-time                2000
                           :ramper/max-urls-per-scheme+authority 500
                           ;; Current estimation of the size of the front in ip addresses. Adaptively
                           ;; increased by the fetching threads whenever they have to wait to retrieve
                           ;; a visit state from the todo queue.
                           :ramper/parsing-threads               2
                           :ramper/required-front-size           1000
                           :ramper/root-dir                      root-dir
                           :ramper/runtime-pause                 false
                           :ramper/runtime-stop                  false
                           :ramper/scheme+authority-delay        2000 ;2 seconds
                           :ramper/sieve-size                    (* 64 1024 1024)
                           ;; TODO for now, as otherwise stuff fails on CircleCI
                           :ramper/store-dir                     (util/temp-dir "store")
                           :ramper/store-buffer-size             (* 64 1024)
                           :ramper/url-cache-max-byte-size       (* 1024 1024 1024)
                           :ramper/user-agent                    "ramper"
                           :ramper/workbench-max-byte-size       (* 512 1024 1024)}))

(defn workbench-size-in-path-queries
  "An estimation of how many path queries should reside in memory."
  ([] (workbench-size-in-path-queries @runtime-config))
  ([runtime-config] (/ (:ramper/workbench-max-byte-size runtime-config) 100)))

(defn stop?
  "Returns true when the agent should stop."
  ([] (stop? @runtime-config))
  ([runtime-config] (:ramper/runtime-stop runtime-config)))

(defn approximate-url-cache-threshold
  "Returns the approximate size for the url cache based on
  `:ramper/url-cache-max-byte-size`."
  ([] (approximate-url-cache-threshold @runtime-config))
  ([runtime-config]
   ;; We store 2 longs per url times an estimator of the memory footprint per long
   (/ (:ramper/url-cache-max-byte-size runtime-config) (* 16 4))))

(defn sieve-dir
  "Returns the sieve directory based on the current runtime-config."
  ([] (sieve-dir @runtime-config))
  ([runtime-config]
   (let [sieve-dir (io/file (:ramper/root-dir runtime-config) "sieve")]
     (when-not (.exists sieve-dir)
       (.mkdirs sieve-dir))
     sieve-dir)))

(defn merge-startup-config
  "Merges a ramper.startup-configuration into a runtime-config atom."
  ([startup-config] (merge-startup-config startup-config runtime-config))
  ([{:ramper/keys [root-dir] :as startup-config} runtime-config]
   (reset! runtime-config
           (merge startup-config
                  {:ramper/is-new        true
                   :ramper/frontier-dir  (io/file root-dir "frontier")
                   :ramper/runtime-pause false
                   :ramper/runtime-stop  false
                   :ramper/store-dir     (io/file root-dir "store")}))))

(comment
  (.mkdirs (io/file (util/project-dir) "store"))
  (.getCanonicalPath (io/file (util/project-dir) "store"))
  )
