(ns ramper.runtime-configuration
  "A certain number of global (configurable) variables that the
  crawler agent has access to via the global accessible atom
  `runtime-config`. Some options come from ramper.startup-configuration,
  others are sensible defaults. Some can be modified via jmx methods
  at runtime."
  (:require [ramper.startup-configuration :as sc]))

(def runtime-config (atom {}))

(defn workbench-size-in-path-queries
  "An estimation of how many path queries should reside in memory."
  []
  (/ (:ramper/workbench-max-byte-size @runtime-config) 100))

(defn stop?
  "Returns true when the agent should stop."
  []
  (:ramper/stop? @runtime-config))
