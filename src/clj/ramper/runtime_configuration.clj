(ns ramper.runtime-configuration
  "A certain number of global (configurable) variables that the
  crawler agent has access to via the global accessible atom
  `runtime-config`. Some options come from ramper.startup-configuration,
  others are sensible defaults. Some can be modified via jmx methods
  at runtime."
  (:require [ramper.startup-configuration :as sc]))

;; these are the default values
(def runtime-config (atom {:ramper/url-cache-max-byte-size (* 1024 1024 1024)}))

(defn workbench-size-in-path-queries
  "An estimation of how many path queries should reside in memory."
  []
  (/ (:ramper/workbench-max-byte-size @runtime-config) 100))

(defn stop?
  "Returns true when the agent should stop."
  []
  (:ramper/stop? @runtime-config))

(defn approximate-url-cache-threshold
  "Returns the approximate size for the url cache based on
  `:ramper/url-cache-max-byte-size`."
  []
  ;; We store 2 longs per url times an estimator of the memory footprint per long
  (/ (:ramper/url-cache-max-byte-size @runtime-config) (* 16 4)))
