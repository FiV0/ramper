(ns ramper.frontier.workbench
  (:require [clojure.math.numeric-tower :as math]
            [io.pedestal.log :as log]
            [lambdaisland.uri]
            [ramper.frontier.workbench.workbench-entry :as we]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util :as util]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.priority-queue :as pq])
  (:import (java.util Arrays)
           (lambdaisland.uri URI)
           (ramper.frontier.workbench.visit_state VisitState)
           (ramper.frontier.workbench.workbench_entry WorkbenchEntry)))

;; Workbench documentation
;;
;; address-to-entry - a map from ip address to workbench-entries (in the workbench)
;; address-to-busy-entries - a map from ip address to workbench entries that are currently busy
;; scheme+authorities - a set of scheme+authorities that have a corresponding visit-state
;; entries - a priority queue based on the next-fetch time of the entries
;; broken - number of broken workbench entries
;; path-queries-count - the total amount of path queries currently in the workbench
;;
;; It's important that the workbench entries and in address-to-entry and entries are
;; in sync, as otherwise the logic fails.
;; The idea is also that all the logic of active visit-states, empty workbench
;; entries, etc... goes *through* the workbench. That doesn't mean you can't
;; compute anything on a workbench entry or a visit state, just make sure that
;; something gets updated, it's reflected in the workbench.

;; TODO: Write good specs to enforce semantics of the workbench.
;; TODO: Think about a way to add to path+queries to the Workbench/Visit-state
;;       even if the visit-state is currently in the Workbench, without it
;;       being too computationally heavy.
;; TODO: the :locked-entry approach as well as keeping up with statitics of items
;;       is extremely fragile
;; TODO: add path-queries inc/dec
(defrecord Workbench [address-to-entry address-to-busy-entry
                      scheme+authorities entries broken path-queries-count])

;; TODO: check why Arrays/hashCode alone might not be good enough
;; add murmurhash3 on top?
(defn hash-ip [ip-address]
  (Arrays/hashCode ip-address))

(defn workbench
  "Creates a new workbench."
  []
  (->Workbench {} {} #{} (pq/priority-queue we/next-fetch) 0 0))

(defn get-workbench-entry
  "Returns a workbench entry for an `ip-address`."
  [^Workbench {:keys [address-to-entry address-to-busy-entry] :as _workbench} ^bytes ip-address]
  {:pre [(= 4 (count ip-address))]}
  (let [ip-hash (hash-ip ip-address)]
    (cond (contains? address-to-entry ip-hash)
          (get address-to-entry ip-hash)
          (contains? address-to-busy-entry ip-hash)
          ;; busy needed
          (-> (get address-to-busy-entry ip-hash)
              (assoc :busy true))
          :else
          (we/workbench-entry ip-address))))

(defn get-workbench-entry-for-visit-state
  "Returns the workbench entry for a given `visit-state`."
  [^Workbench workbench ^VisitState {:keys [ip-address] :as visit_state}]
  {:pre [(contains? visit_state :ip-address)]}
  (get-workbench-entry workbench ip-address))

(defn nb-workbench-entries
  "Returns the number of workbench entries in the `workbench`."
  [^Workbench {:keys [address-to-entry address-to-busy-entry] :as _workbench}]
  (+ (count address-to-entry) (count address-to-busy-entry)))

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
  (if (and (seq entries) (<= (-> entries peek we/next-fetch) (System/currentTimeMillis)))
    (-> entries peek we/first-visit-state)
    nil))

(defn- update-workbench
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

(defn set-entry-next-fetch
  "Sets the next-fetch time of workbench entry."
  [^Workbench {:keys [address-to-entry address-to-busy-entry] :as workbench}
   ^bytes ip-address next-fetch]
  {:pre [(= 4 (count ip-address))]}
  (let [ip-hash (hash-ip ip-address)]
    (cond-let [old-wb-entry (get address-to-entry ip-hash)]
              (update-workbench workbench old-wb-entry (assoc old-wb-entry :next-fetch next-fetch))
              [old-wb-entry (get address-to-busy-entry ip-hash)]
              ;; here we assume old-wb-entry is not in entries
              (update workbench :address-to-busy-entry assoc ip-hash (assoc old-wb-entry :next-fetch next-fetch))
              :else
              (throw (IllegalStateException. (str "ip address: " (util/ip-address->str ip-address) " not in workbench!!!"))))))

(defn add-scheme+authority
  "Signals to the `workbench` that a visit-state has been created for `scheme+authority`
  has been created. See also scheme+authority-present?."
  [^Workbench workbench scheme+authority]
  (update workbench :scheme+authorities conj scheme+authority))

(defn scheme+authority-present?
  "Returns true when there exists an active visit-state for the schem+authority."
  [^Workbench {:keys [scheme+authorities] :as _workbench} ^URI scheme+authority]
  (contains? scheme+authorities scheme+authority))

(defn pop-visit-state
  "Removes the next visit state from the `workbench`.

  Does not validate if an visit-state can be popped! Be aware that this should
  be called in conjunction with `peek-visit-state` or best with `dequeue-visit-state`,
  to keep the internals of the workbench intact."
  [^Workbench {:keys [entries] :as workbench}]
  (let [{:keys [ip-address] :as old-wb-entry} (peek entries)
        new-wb-entry (we/remove old-wb-entry)
        ip-hash (hash-ip ip-address)]
    (-> workbench
        (update :entries dissoc old-wb-entry)
        (update :address-to-entry dissoc ip-hash)
        (update :address-to-busy-entry assoc ip-hash new-wb-entry))))

(defn add-visit-state
  "Adds a `visit-state` to `workbench`, creating a new workbench-entry if necessary.
  The visit-state might be a new visit-state or an old one that has passed through the
  workbench."
  [^Workbench {:keys [address-to-entry address-to-busy-entry] :as workbench}
   ^VisitState {:keys [ip-address locked-entry scheme+authority] :as visit-state}]
  {:pre [(contains? visit-state :ip-address)]}
  (let [ip-hash (hash-ip ip-address)
        {:keys [busy] :as old-wb-entry} (get-workbench-entry workbench ip-address)
        old-wb-entry (dissoc old-wb-entry :busy)
        new-wb-entry (we/add old-wb-entry (dissoc visit-state :locked-entry))
        workbench (update workbench :scheme+authorities conj scheme+authority)] ; still needed?
    (when locked-entry
      (assert (contains? address-to-busy-entry ip-hash))
      (assert (not (contains? address-to-entry ip-hash))))
    (cond
      ;; workbench entry busy, but needs to get released
      locked-entry
      ;; TODO calculate hash only once
      (-> workbench
          (update :address-to-busy-entry dissoc ip-hash)
          (update :address-to-entry assoc ip-hash new-wb-entry)
          (update :entries conj new-wb-entry))
      ;; workbench entry busy, but cannot be released
      busy
      (update workbench :address-to-busy-entry assoc ip-hash new-wb-entry)
      ;; workbench entry not busy, standard update
      :else
      (update-workbench workbench old-wb-entry new-wb-entry))))

(defn purge-visit-state
  "Should be called when a visit-state is not readded to the workbench after
  having been popped."
  [^Workbench {:keys [address-to-busy-entry] :as workbench}
   ^VisitState {:keys [ip-address scheme+authority] :as _visit-state}]
  (let [ip-hash (hash-ip ip-address)
        new-workbench (update workbench :scheme+authorities disj scheme+authority)]
    (if-let [new-wb-entry (get address-to-busy-entry ip-hash)]
      (let [new-workbench (update new-workbench :address-to-busy-entry dissoc ip-hash)]
        (if (not (we/empty? new-wb-entry))
          (-> new-workbench
              (update :address-to-entry assoc ip-hash new-wb-entry)
              (update :entries conj new-wb-entry))
          new-workbench))
      new-workbench)))

(defn dequeue-visit-state!
  "Takes a workbench atom and dequeues the first available visit-state
  (if any) and updates the atom accordingly."
  [workbench-atom]
  (loop []
    (let [wb     @workbench-atom
          value (peek-visit-state wb)
          new-wb (pop-visit-state wb)]
      (cond (nil? value)
            nil
            (and value (compare-and-set! workbench-atom wb new-wb))
            (assoc value :locked-entry true)
            :else (recur)))))

;; Copied from BUbing
(defn path-query-limit
  "Calculates the number of path-queries the given `visit-state` should keep in memory."
  [^Workbench {:keys [address-to-entry address-to-busy-entry] :as _workbench}
   ^VisitState {:keys [ip-address] :as visit-state}
   {:ramper/keys [ip-delay scheme+authority-delay] :as runtime-config}
   required-front-size]
  {:pre [(contains? visit-state :ip-address)
         (contains? (merge address-to-entry address-to-busy-entry) (hash-ip ip-address))]}
  (let [workbench-entry (get (merge address-to-entry address-to-busy-entry) (hash-ip ip-address))
        delay-ratio (max 1.0 (/ (+ scheme+authority-delay 1.0)
                                (+ ip-delay 1.0)))
        scaling-factor (if (nil? workbench-entry) 1.0 (/ (we/size workbench-entry) delay-ratio))]
    (min (if (zero? scheme+authority-present?) Double/MAX_VALUE (/ 300000 scheme+authority-delay))
         (max 4 (math/ceil (/ (runtime-config/workbench-size-in-path-queries runtime-config)
                              (* scaling-factor required-front-size)))))))
