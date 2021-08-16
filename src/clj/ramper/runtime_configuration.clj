(ns ramper.runtime-configuration
  "A certain number of global (configurable) variables that the
  crawler agent has access to via the global accessible atom
  `runtime-config`. Some options come from ramper.startup-configuration,
  others are sensible defaults. Some can be modified via jmx methods
  at runtime."
  (:require [clojure.java.io :as io]
            [ramper.startup-configuration :as sc]
            [ramper.util :as util]))

(def ^:private root-dir (util/temp-dir "ramper-root"))

;; these are the default values
(def runtime-config (atom {:ramper/user-agent "ramper"
                           :ramper/keepalive-time 2000 ;;
                           :ramper/runtime-stop false
                           :ramper/runtime-pause false
                           :ramper/cookies-max-byte-size 2048
                           :ramper/url-cache-max-byte-size (* 1024 1024 1024)
                           :ramper/root-dir root-dir
                           :ramper/sieve-size (* 64 1024 1024)
                           :ramper/store-buffer-size (* 64 1024)
                           :ramper/aux-buffer-size (* 64 1024)
                           :ramper/ip-delay 2000 ;2 seconds
                           :ramper/scheme+authority-delay 2000 ;2 seconds
                           :ramper/frontier-dir (io/file root-dir "frontier")
                           :ramper/max-urls-per-scheme+authority 500
                           :ramper/store-dir (io/file (util/project-dir) "store")
                           :ramper/is-new true}))

(defn workbench-size-in-path-queries
  "An estimation of how many path queries should reside in memory."
  []
  (/ (:ramper/workbench-max-byte-size @runtime-config) 100))

(defn stop?
  "Returns true when the agent should stop."
  ([] (stop? @runtime-config))
  ([runtime-config] (:ramper/runtime-stop? runtime-config)))

(defn approximate-url-cache-threshold
  "Returns the approximate size for the url cache based on
  `:ramper/url-cache-max-byte-size`."
  []
  ;; We store 2 longs per url times an estimator of the memory footprint per long
  (/ (:ramper/url-cache-max-byte-size @runtime-config) (* 16 4)))

(defn sieve-dir
  "Returns the sieve directory based on the current runtime-config."
  []
  (let [sieve-dir (io/file (:ramper/root-dir @runtime-config) "sieve")]
    (when-not (.exists sieve-dir)
      (.mkdirs sieve-dir))
    sieve-dir))
