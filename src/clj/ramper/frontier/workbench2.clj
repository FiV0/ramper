(ns ramper.frontier.workbench2
  (:require [clojure.math.numeric-tower :as math]
            [io.pedestal.log :as log]
            [lambdaisland.uri :as uri]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util :as util]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.priority-queue :as pq])
  (:import (java.net InetAddress)
           (java.util Arrays)
           (lambdaisland.uri URI)))

;; forget about ip-delay for now

(defrecord Entry [scheme+authority ip-address next-fetch cookies
                  last-exception retries path-queries]
  Object
  (toString [_this] (str "#Entry[" scheme+authority "(" (count path-queries) ")]")))

(defn entry
  ([scheme+authority] (entry scheme+authority []))
  ([scheme+authority path-queries]
   (->Entry scheme+authority nil nil nil nil 0 (into clojure.lang.PersistentQueue/EMPTY path-queries))))

(defn first-url [{:keys [scheme+authority path-queries]}]
  (when-let [path-query (peek path-queries)]
    (str scheme+authority path-query)))

(defn pop-url [entry] (update entry :path-queries pop))

(defrecord Workbench [base->path-queries entries])

(defn workbench []
  (->Workbench {} (pq/priority-queue :next-fetch)))

(defn nb-workbench-entries
  "Returns the number of workbench entries in the `workbench`."
  [^Workbench {:keys [base->path-queries] :as _workbench}]
  (count base->path-queries))

(defn add-scheme+authority
  "Signals to the `workbench` that an entry has been created for `scheme+authority`
  has been created. See also scheme+authority-present?."
  [^Workbench {:keys [base->path-queries] :as workbench} scheme+authority]
  (update workbench :base->path-queries assoc scheme+authority (or (get base->path-queries scheme+authority)
                                                                   clojure.lang.PersistentQueue/EMPTY)))

(defn scheme+authority-present?
  "Returns true when there exists an active entry for the schem+authority."
  [^Workbench {:keys [base->path-queries] :as _workbench} ^URI scheme+authority]
  (contains? base->path-queries scheme+authority))

(defn add-entry
  "Adds a `entry` to the workbench."
  [{:keys [base->path-queries] :as workbench} {:keys [ip-address scheme+authority] :as entry}]
  {:pre [(not (nil? ip-address))]}
  (-> workbench
      (add-scheme+authority scheme+authority)
      (update :entries conj (update entry :path-queries #(into %1 %2) (get base->path-queries scheme+authority)))))

(defn add-path-query
  "Adds a `path-query` to the workbench."
  [workbench scheme+authority path-query]
  (update workbench :base->path-queries
          update scheme+authority (fnil conj clojure.lang.PersistentQueue/EMPTY) path-query))

(defn queued-path-queries?
  "Checks weather there are currently queued path-queries in the workbench."
  [{:keys [base->path-queries] :as _workbench} scheme+authority]
  (seq (get base->path-queries scheme+authority)))

(defn peek-entry
  "Returns the next entry that is available in the `workbench`, nil
  if there is none available."
  [^Workbench {:keys [entries] :as _workbench}]
  (if-let [[entry] (seq entries)]
    (when (<= (:next-fetch entry) (System/currentTimeMillis))
      entry)
    nil))

(defn pop-entry
  "Removes the next entry from the `workbench`.

  Does not validate if an entry can be popped! Be aware that this should
  be called in conjunction with `peek-entry` or best with `dequeue-entry`,
  to keep the internals of the workbench intact."
  [workbench]
  (update workbench :entries pop))

(defn purge-entry
  "Should be called when a entry is not readded to the workbench after
  having been popped."
  [^Workbench workbench {:keys [scheme+authority] :as _entry}]
  (update workbench :base->path-queries dissoc scheme+authority))

(defn dequeue-entry!
  "Takes a workbench atom and dequeues the first available visit-state
  (if any) and updates the atom accordingly."
  [workbench-atom]
  (loop []
    (let [wb     @workbench-atom
          value (peek-entry wb)]
      (cond (nil? value)
            nil
            (and value (compare-and-set! workbench-atom wb (pop-entry wb)))
            value
            :else (recur)))))

(defn path-query-limit
  "Calculates the number of path-queries the given `visit-state` should keep in memory."
  [^Workbench {:keys [_base->path-queries] :as _workbench}
   {:keys [] :as _entry}
   {:ramper/keys [_ip-delay _scheme+authority-delay] :as _runtime-config}
   _required-front-size]
  ;; TODO
  500)
