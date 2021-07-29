(ns ramper.frontier.workbench
  (:require [ramper.util.priority-queue :as pq]
            [ramper.frontier.workbench.workbench-entry :as we])
  (:import (java.util Arrays)
           (ramper.frontier.workbench.visit_state VisitState)
           (ramper.frontier.workbench.workbench_entry WorkbenchEntry)))

;; Workbench documentation
;;
;; address-to-entry - a map from ip address to workbench-entries (in the workbench)
;; entries - a priority queue based on the next-fetch time of the entries
;; broken - number of broken workbench entries
;;
;; It's important that the workbench entries and in address-to-entry and entries are
;; in sync, as otherwise the logic fails.
;; We also currently assume that a WorkbenchEntry in

(defrecord Workbench [address-to-entry entries broken])

;; TODO: check why Arrays/hashCode alone might not be good enough
;; add murmurhash3 on top?
(defn- hash-ip [ip-address]
  (Arrays/hashCode ip-address))

(defn workbench
  "Creates a new workbench."
  []
  (->Workbench {} (pq/priority-queue we/next-fetch) 0))

(defn get-workbench-entry
  "Returns a workbench entry for an `ip-address`."
  [^Workbench workbench ^bytes ip-address]
  {:pre [(= 4 (count ip-address))]}
  (get workbench (hash-ip ip-address) (we/workbench-entry ip-address)))

(defn get-workbench-entry-for-visit-state
  "Returns the workbench entry for a given `visit-state`."
  [^Workbench workbench ^VisitState {:keys [ip-address] :as visit_state}]
  {:pre [(contains? :ip-address visit_state)]}
  (get-workbench-entry workbench ip-address))

(defn nb-workbench-entries
  "Returns the number of workbench entries in the `workbench`."
  [^Workbench {:keys [address-to-entry] :as _workbench}]
  (count address-to-entry))

(defn add-workbench-entry
  "Adds a `workbench-entry` to the workbench."
  [^Workbench workbench ^WorkbenchEntry workbench-entry]
  (-> workbench
      (update :address-to-entry assoc (hash-ip (:ip-address workbench-entry)) workbench-entry)
      (update :entries conj workbench-entry)))

(defn peek-visit-state
  "Returns the next visit state that is available in the `workbench`, nil
  if there is none available."
  [^Workbench {:keys [entries] :as _workbench}]
  (if (<= (-> entries peek we/next-fetch) (System/currentTimeMillis) )
    (-> entries peek we/first-visit-state)
    nil))

(defn update-workbench
  "Removes the workbench entry `old-wb-entry` from the `workbench` and adds the
  workbench entry `new-wb-entry`."
  [^Workbench {:keys [entries address-to-entry] :as workbench} old-wb-entry new-wb-entry]
  {:pre [(= (:ip-address old-wb-entry) (:ip-address new-wb-entry))]}
  (let [ip-hash (hash-ip (:ip-address old-wb-entry))
        new-entries (-> entries
                        (dissoc old-wb-entry)
                        (conj new-wb-entry))
        new-address-to-entry (-> address-to-entry
                                 (dissoc ip-hash)
                                 (assoc ip-hash new-wb-entry))]
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

(defn dequeue-visit-state!
  "Takes a workbench atom and dequeues the first available visit-state
  (if any) and updates the atom accordingly."
  [workbench-atom]
  (loop []
    (let [wb     @workbench-atom
          value (peek-visit-state wb)
          new-wb (pop-visit-state wb)]
      (cond (nil? value) nil
            (and value (compare-and-set! workbench-atom wb new-wb)) value
            :else (recur)))))
