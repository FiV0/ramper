(ns ramper.workers.done-thread
  (:require [io.pedestal.log :as log]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.persistent-queue :as pq]
            [ramper.util.thread :as thread-utils])
  (:import (ramper.frontier Entry Workbench3)))

(def ^:private the-ns-name (str *ns*))

(defn done-thread
  "Moves visit states from the done queue to the workbench or the
  refill queue. If no more path queries (urls) are available purges
  them from the workbench.

  The given thread-data map must contain:

  :runtime-config - an atom containing the runtime configuration, see
  also ramper.runtime-configuration/runtime-config.

  :workbench - an atom containing the workbench of the agent

  :done-queue - an atom wrapping a clojure.lang.PersistentQueue from which the available
  visit states will be dequeued

  :refill-queue - an atom wrapping a clojure.lang.PersistentQueue to which the refillable
  visit states will be enqueued

  :virtualizer - a workbench virtualizer, see also
  ramper.frontier.workbench.virtualizer"
  [{:keys [runtime-config ^Workbench3 workbench done-queue
           refill-queue virtualizer] :as _thread_data}]
  (thread-utils/set-thread-name the-ns-name)
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (loop [i 0]
      (when-not (runtime-config/stop? @runtime-config)
        (if-let [^Entry entry (pq/dequeue! done-queue)]
          (let [next-fetch (.getNextFetch entry)
                size (.size entry)]
            (cond
              ;; purge condition
              (= Long/MAX_VALUE next-fetch)
              (.purgeEntry workbench entry)

              ;; refill condition
              ;; TODO: test if this needs to pass through the distributor, why not go directly
              ;; to the workbench
              (and (= 0 size)
                   (< 0 (virtual/count virtualizer entry)))
              (swap! refill-queue conj entry)

              ;; we also purge if there no more urls on disk
              (= 0 size)
              (.purgeEntry workbench entry)

              :else
              (.addEntry workbench entry))
            (recur 0))
          (do
            (Thread/sleep (bit-shift-left 1 (max i 10)))
            (recur (inc i))))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :done-thread :graceful-shutdown)
  true)
