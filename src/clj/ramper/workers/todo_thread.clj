(ns ramper.workers.todo-thread
  (:require [io.pedestal.log :as log]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.thread :as thread-utils])
  (:import (ramper.frontier Entry Workbench3)))

(def ^:private the-ns-name (str *ns*))

(defn todo-thread
  "This function executes the logic of moving visit-states from the
  workbench to the todo-queue.

  The given thread-data map must contain:

  :runtime-config - an atom containing the runtime configuration, see
  also ramper.runtime-configuration/runtime-config.

  :workbench - an atom containing the workbench of the agent

  :todo-queue - a clojure.lang.PersistentQueue to which the available
  visit states will be added

  :scheme+authority-to-count - an atom wrapping a map from scheme+authority to the
  number of path queries that have passed through the workbench"
  [{:keys [runtime-config ^Workbench3 workbench
           todo-queue scheme+authority-to-count] :as _thread_data}]
  (thread-utils/set-thread-name the-ns-name)
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (while (not (runtime-config/stop? @runtime-config))
      ;; TODO maybe make the dequeue loop explict to enable logging
      ;; TODO maybe enable backoff, check if loop spins
      (when-let [^Entry entry (.popEntry workbench 100)]
        (let [scheme+authority (.-schemeAuthority entry)]
          (assert (<= (get @scheme+authority-to-count scheme+authority 0)
                      (:ramper/max-urls-per-scheme+authority @runtime-config)))
          (swap! todo-queue conj entry))))
    (catch Throwable t
      (log/error :unexpected-ex {:ex t})))
  (log/info :todo-thread :graceful-shutdown)
  true)
