(ns ramper.workbench
  (:require [ramper.util.priority-queue :as pq]
            [ramper.workbench.workbench-entry :as we])
  (:import (ramper.workbench.workbench_entry WorkbenchEntry)))

;; Workbench documentation
;;
;; address-to-entry - a map from ip address to workbench-entries
;; entries - a priority queue based on the next-fetch time of the entries
;; broken - number of broken workbench entries

(defrecord Workbench [address-to-entry entries broken])

(defn workbench
  "Creates a new workbench."
  []
  (->Workbench {} (pq/priority-queue we/next-fetch) 0))

(defn get-workbench-entry
  "Returns a workbench entry for an `ip-address`."
  [^Workbench workbench ^bytes ip-address]
  {:pre [(= 4 (.length ip-address))]}
  (get workbench ip-address))

(defn nb-workbench-entries
  "Returns the number of workbench entries in the `workbench`."
  [^Workbench {:keys [address-to-entry] :as _workbench}]
  (count address-to-entry))

(defn add
  "Adds a `workbench-entry` to the workbench."
  [^Workbench workbench ^WorkbenchEntry workbench-entry]
  (-> workbench
      (update :address-to-entry assoc (:ip-address workbench-entry) workbench-entry)
      (update :entries conj workbench-entry)))

(defn get-visit-state
  "Returns the next visit state that is available in the `workbench`, nil
  if there is none available."
  [^Workbench {:keys [entries] :as _workbench}]
  (if (<= (System/currentTimeMillis) (-> entries peek we/next-fetch))
    (-> entries peek we/first-visit-state)
    nil))

(defn pop-visit-state
  "Removes the next visit state from the `workbench`.
  Does not check if valid or not!"
  [^Workbench workbench]
  (update workbench :entries we/remove))
