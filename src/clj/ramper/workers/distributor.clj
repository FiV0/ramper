(ns ramper.workers.distributor
  "The code for the distributor thread of ramper."
  (:require [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.frontier :as frontier]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.sieve :as sieve]
            [ramper.sieve.disk-flow-receiver :as flow-receiver]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.persistent-queue :as queue-utils]
            [ramper.util.thread :as thread-utils]))

(defn- front-too-small? [workbench todo-queue required-front-size]
  (<= (+ (- (workbench/nb-workbench-entries workbench)
            (count (:broken-visit-states workbench)))
         (count todo-queue))
      required-front-size))

;; no need for a stop channel as there is only one distributor
(defn distributor-thread [{:keys [workbench todo-queue refill-queue required-front-size
                                  virtualizer sieve runtime-config ready-urls] :as _thread-data}]
  (thread-utils/set-thread-name (str *ns*))
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (loop [round 0 stats {:moved-from-queues         0
                          :deleted-from-queues       0
                          :full-workbench-sleep-time 0
                          :large-front-sleep-time    0
                          :no-ready-urls-sleep-time  0
                          :from-sieve-to-virtualizer 0
                          :from-sieve-to-overflow    0
                          :from-sieve-to-workbench   0
                          :deleted-from-sieve        0}]
      (when-not (runtime-config/stop?)
        (let [workbench-full (frontier/workbench-full?)
              front-too-small (front-too-small? @workbench @todo-queue @required-front-size)
              now (System/currentTimeMillis)]
          (cond (not workbench-full)
                (do
                  ;; stopping here when flushing
                  (locking @sieve)
                  (cond-let [visit-state (queue-utils/dequeue! refill-queue)]
                            (if (= 0 (virtual/count virtualizer visit-state))
                              (log/info :distributor/no-urls {:visit-state (dissoc visit-state :path-queries)})
                              (let [path-query-limit (workbench/path-query-limit
                                                      @workbench visit-state @runtime-config @required-front-size)

                                    new-visit-state (virtual/dequeue-path-queries
                                                     virtualizer visit-state path-query-limit)]
                                ;; TODO move new-visit-state back to workbench
                                (recur 0 (update stats :moved-from-queues + (- (-> new-visit-state :path-queries count)
                                                                               (-> visit-state :path-queries count))))))

                            (and front-too-small
                                 (zero? (flow-receiver/size @ready-urls))
                                 (>= now (+ (sieve/last-flush @sieve) constants/min-flush-interval)))
                            (do
                              (sieve/flush @sieve)
                              (recur 0 stats))

                            (and front-too-small (pos? (flow-receiver/size @ready-urls)))
                            :todo

                            :else
                            :todo))

                (not= 0 round)
                (let [sleep-time (bit-shift-left 1 (min 10 round))]
                  (Thread/sleep sleep-time)
                  (cond (not front-too-small)
                        (recur (inc round) (update stats :large-front-sleep-time + sleep-time))
                        workbench-full
                        (recur (inc round) (update stats :full-workbench-sleep-time + sleep-time))
                        :else
                        (recur (inc round) (update stats :no-ready-urls-sleep-time + sleep-time))))
                :else
                (recur (inc round) stats)))))

    (catch Throwable t
      (log/error :unexpected-ex (Throwable->map t))))
  (log/info :distributor-thread :graceful-shutdown)
  true)
