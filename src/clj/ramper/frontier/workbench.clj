(ns ramper.frontier.workbench
  (:require [ramper.util.priority-queue :as pq]
            [ramper.frontier.workbench.workbench-entry :as we])
  (:import (ramper.frontier.workbench.visit_state VisitState)
           (ramper.frontier.workbench.workbench_entry WorkbenchEntry)))

;; Workbench documentation
;;
;; address-to-entry - a map from ip address to workbench-entries (in the workbench)
;; entries - a priority queue based on the next-fetch time of the entries
;; broken - number of broken workbench entries
;;
;; It's important that the workbench entries and in address-to-entry and entries are
;; in sync, as otherwise the logic fails.

(defrecord Workbench [address-to-entry entries broken])

(defn workbench
  "Creates a new workbench."
  []
  (->Workbench {} (pq/priority-queue we/next-fetch) 0))

(defn get-workbench-entry
  "Returns a workbench entry for an `ip-address`."
  [^Workbench workbench ^bytes ip-address]
  {:pre [(= 4 (.length ip-address))]}
  (get workbench ip-address (we/workbench-entry ip-address)))

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

(defn- update-workbench [^Workbench {:keys [entries address-to-entry] :as workbench} old-wb-entry new-wb-entry]
  (let [new-entries (-> entries
                        (dissoc old-wb-entry)
                        (conj new-wb-entry))
        new-address-to-entry (-> address-to-entry
                                 (dissoc (:ip-address old-wb-entry))
                                 (assoc (:ip-address new-wb-entry) new-wb-entry))]
    (-> workbench
        (assoc :entries new-entries)
        (assoc :address-to-entry new-address-to-entry))))

(defn pop-visit-state
  "Removes the next visit state from the `workbench`.
  Does not check if valid or not!"
  [^Workbench {:keys [entries] :as workbench}]
  (let [old-wb-entry (peek entries)
        new-wb-entry (we/remove old-wb-entry)]
    (update-workbench workbench old-wb-entry new-wb-entry)))

(defn add-visit-state
  "Adds a new `visit-state` to `workbench`, creating a new workbench-entry if necessary."
  [^Workbench workbench
   ^VisitState {:keys [ip-address] :as visit-state}]
  {:pre [(contains? visit-state :ip-address)]}
  (let [old-wb-entry (get-workbench-entry workbench ip-address)
        new-wb-entry (we/add old-wb-entry visit-state)]
    (update-workbench workbench old-wb-entry new-wb-entry)))
