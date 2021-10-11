(ns ramper.workers.done-thread
  (:require [io.pedestal.log :as log]
            [ramper.frontier.workbench2 :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.persistent-queue :as pq]
            [ramper.util.thread :as thread-utils]))

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
  [{:keys [runtime-config workbench done-queue
           refill-queue virtualizer] :as _thread_data}]
  (thread-utils/set-thread-name the-ns-name)
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (loop [i 0]
      (when-not (runtime-config/stop? @runtime-config)
        (if-let [{:keys [next-fetch path-queries] :as entry} (pq/dequeue! done-queue)]
          (do
            (cond
              ;; purge condition
              (= Long/MAX_VALUE next-fetch)
              (swap! workbench workbench/purge-entry entry)

              ;; just readd to workbench
              (seq path-queries)
              (swap! workbench workbench/add-entry entry)

              ;; refill condition
              ;; TODO: test if this needs to pass through the distributor, why not go directly
              ;; to the workbench
              (< 0 (virtual/count virtualizer entry))
              (swap! refill-queue conj entry)

              ;; o/w we purge as there are no more urls on disk
              :else
              (swap! workbench workbench/purge-entry entry))
            (recur 0))
          (do
            (Thread/sleep (bit-shift-left 1 (max i 10)))
            (recur (inc i))))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :done-thread :graceful-shutdown)
  true)
