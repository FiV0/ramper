(ns ramper.frontier.workbench.workbench-entry
  (:refer-clojure :exclude [remove empty?])
  (:require [ramper.util.priority-queue :as pq]
            [ramper.frontier.workbench.visit-state])
  (:import (ramper.frontier.workbench.visit_state VisitState)))

;; workbench-entry documentation
;;
;; visit-states - a priority queue of visit states
;; ip-address - the ip address associated with this workbench entry
;; broken-visit-states - number of broken visit states in visit-states
;; next-fetch - next fecht time this ip address based on ip politeness
;;              constraints, be aware this might be different to the
;;              next-fetch timestamp of the visit-states

(defrecord WorkbenchEntry [visit-states ip-address broken-visit-states next-fetch])

(defn workbench-entry
  "Initializes a workbench entry with the given `ip-address`."
  [^bytes ip-address]
  {:pre [(= 4 (.length ip-address))]}
  (->WorkbenchEntry (pq/priority-queue :next-fetch) ip-address 0 0))

(defn is-broken?
  "Checks whether the workbench entry is completely broken."
  [^WorkbenchEntry {:keys [visit-states broken-visit-states] :as _workbench-entry}]
  (and (< 0 broken-visit-states) (= broken-visit-states (count visit-states))))

(defn add
  "Adds a `visit-state` to the `workbench-entry`."
  [^WorkbenchEntry workbench-entry ^VisitState visit-state]
  (update workbench-entry :visit-states conj visit-state))

(defn remove
  "Removes the top visit-state from the `workbench-entry`."
  [^WorkbenchEntry workbench-entry]
  (update workbench-entry :visit-states pop))

(defn first-visit-state
  "Returns the first visit state in the `workbench-entry`."
  [^WorkbenchEntry workbench-entry]
  (-> workbench-entry :visit-states peek))

(defn size
  "Returns the size of the workbench entry"
  [^WorkbenchEntry workbench-entry]
  (-> workbench-entry :visit-states count))

(defn empty?
  "Returns the size of the workbench entry"
  [^WorkbenchEntry workbench-entry]
  (= 0 (size workbench-entry)))

(defn next-fetch
  "Returns the minimum time when some url from this `workbench-entry` can
  be accessed."
  [^WorkbenchEntry {:keys [next-fetch] :as workbench-entry}]
  (max next-fetch (-> workbench-entry first-visit-state :next-fetch)))
