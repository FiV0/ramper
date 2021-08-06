(ns ramper.workers.todo-thread
  (:require [io.pedestal.log :as log]
            [ramper.frontier.workbench :as workbench]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.util.thread :as thread-utils]))

(defn todo-thread [{:keys [runtime-config workbench
                           todo-queue scheme+authority-to-count] :as _thread_data}]
  (thread-utils/set-thread-name (str *ns*))
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (while (runtime-config/stop? @runtime-config)
      ;; TODO maybe make the dequeue loop explict to enable logging
      (let [{:keys [scheme+authority] :as visit-state} (workbench/dequeue-visit-state! workbench)]
        (assert (<= (get scheme+authority-to-count scheme+authority 0)
                    (:ramper/max-urls-per-scheme+authority @runtime-config)))
        (swap! todo-queue conj visit-state)))
    (catch Throwable t
      (log/error :unexpected-ex (Throwable->map t))))
  (log/info :todo-thread :graceful-shutdown)
  true)
