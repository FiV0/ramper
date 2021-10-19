(ns ramper.workers.distributor
  "The code for the distributor thread of ramper."
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.frontier :as frontier]
            [ramper.frontier.workbench :as workbench]
            [ramper.frontier.workbench.virtualizer :as virtual]
            [ramper.frontier.workbench.visit-state :as visit-state]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.sieve :as sieve]
            [ramper.sieve.disk-flow-receiver :as flow-receiver]
            [ramper.util.macros :refer [cond-let]]
            [ramper.util.persistent-queue :as queue-utils]
            [ramper.util.thread :as thread-utils]
            [ramper.util.url :as url]))

(defn- front-too-small? [workbench required-front-size]
  (< (.numberOfEntries workbench) required-front-size))

;; TODO should this be configurable
(def front-too-small-loop-size 100)

(defn enlarge-front
  "Dequeues keys from the sieve (through a flow receiver) and either drops them
  (already too many urls parsed for a domain), add the url to workbench virtualizer
  (a workbench entry exists and is in circulation) or creates a new entry
  for new domains.
  For the given thread-data map see r.workers.distributor/distributor-thread."
  [{:keys [workbench virtualizer runtime-config scheme+authority-to-count
           ready-urls new-entries] :as _thread-data} stats]
  (loop [cnt 0 stats stats scheme+authority-to-new-entries {}]
    (if (and (< cnt front-too-small-loop-size)
             (pos? (flow-receiver/size ready-urls)))
      (let [url (flow-receiver/dequeue-key ready-urls)
            scheme+authority (str (url/scheme+authority url))
            path+query (str (url/path+queries url))]
        (cond
          ;; url gets dropped
          (not (< (get @scheme+authority-to-count scheme+authority 0)
                  (:ramper/max-urls-per-scheme+authority @runtime-config)))
          (recur (inc cnt)
                 (update stats :deleted-from-sieve inc)
                 scheme+authority-to-new-entries)

          ;; scheme+authority already has an entry
          ;; TODO coordinate with path-query-limit
          (.schemeAuthorityPresent workbench scheme+authority)
          (let [entry (.getEntry workbench scheme+authority)]
            (if (< (.size entry) 1000)
              (do (.addPathQuery entry path+query)
                  (recur (inc cnt)
                         (update stats :from-sieve-to-workbench inc)
                         scheme+authority-to-new-entries))
              ;; has an entry but the number of path queries exceeds the limit
              ;; TODO make the virtualizer interface better
              ;; this is unnecessary
              (do
                (virtual/enqueue virtualizer entry url)
                (recur (inc cnt)
                       (update stats :from-sieve-to-virtualizer inc)
                       scheme+authority-to-new-entries))))

          ;; we create or update a new entry
          ;; TODO with the non persistent workbench we actually always land in the
          ;; nil case, as the case above will be true afterwards.
          :else
          (recur (inc cnt)
                 (update stats :from-sieve-to-workbench inc)
                 (update scheme+authority-to-new-entries
                         scheme+authority
                         (fnil #(do (.addPathQuery %1 %2) %1) (.getEntry workbench scheme+authority))
                         path+query))))
      (do
        (swap! new-entries into (vals scheme+authority-to-new-entries))
        stats))))

(def ^:private the-ns-name (str *ns*))

(defn distributor-thread
  "The distributor thread takes care of refilling the workbench, passing new
  visit states to the dns threads and adding urls to the virtualizer.

  The thread-data map must contain:

  :workbench - an atom wrapping the agents workbench.

  :todo-queue - an atom wrapping a clojure.lang.PersistentQueue from which ready visit
  states can be dequeued.

  :refill-queue - an atom wrapping a clojure.lang.PersistentQueue to which the refillable
  visit states will be enqueued.

  :virtualizer - a workbench virtualizer, see also
  ramper.frontier.workbench.virtualizer.

  :sieve - a sieve satisfying r.sieve/Sieve

  :runtime-config - an atom wrapping the runtime config of the agent.

  :ready-urls - a flow receiver of urls (as strings) that have passed the sieve
  and implementing r.sieve.flow-receiver/FlowReceiver.

  :scheme+authority-to-count - an atom wrapping a mapping from scheme+authority to the
  number of path queries that have passed through the workbench.

  :new-visit-state - an atom wrapping a clojure.lang.PersistentQueue to which the new
  visit states (without resolved ip-address) will be enqueued.

  :path-queries-in-queues - an atom wrapping a counter for the number of
  path-queries in visit states."
  [{:keys [workbench _todo-queue refill-queue
           virtualizer sieve runtime-config ready-urls
           _scheme+authority-to-count _new-visit-states
           path-queries-in-queues stats-chan] :as thread-data}]
  (thread-utils/set-thread-name the-ns-name)
  (thread-utils/set-thread-priority Thread/MAX_PRIORITY)
  (try
    (loop [round 0 stats {:moved-from-queues         0
                          :full-workbench-sleep-time 0
                          :large-front-sleep-time    0
                          :no-ready-urls-sleep-time  0
                          :from-sieve-to-virtualizer 0
                          :from-sieve-to-workbench   0
                          :deleted-from-sieve        0
                          :entries-purged       0}]
      (when-not (runtime-config/stop? @runtime-config)
        (let [required-front-size (:ramper/required-front-size @runtime-config)
              workbench-full (frontier/workbench-full? @runtime-config @path-queries-in-queues)
              front-too-small (front-too-small? workbench required-front-size)
              now (System/currentTimeMillis)]
          ;; TODO should this only be done at certain time intervals?
          (async/offer! stats-chan stats)
          ;; TODO try to refactor this in one big cond
          (cond (not workbench-full)
                (do
                  ;; stopping here when flushing
                  (locking sieve)
                  (cond-let [entry (queue-utils/dequeue! refill-queue)]
                            (let [entry-size (.size entry)
                                  virtual-empty (= 0 (virtual/count virtualizer entry))]
                              (cond
                                ;; nothing on disk and visit-state is empty
                                (and (= 0 entry-size) virtual-empty)
                                (do
                                  (log/info :distributor/purge {:entry entry})
                                  (.purgeEntry workbench entry)
                                  (recur 0 (update stats :entries-purged inc)))
                                ;; nothing on disk but visit-state still contains urls
                                virtual-empty
                                (do
                                  (log/info :distributor/no-urls {:entry entry})
                                  (.addEntry workbench entry)
                                  (recur 0 stats))
                                ;; we refill the visit state
                                :else
                                (let [path-query-limit (.pathQueryLimit workbench)
                                      new-entry (virtual/dequeue-path-queries
                                                 virtualizer entry path-query-limit)
                                      new-entry-size (.size new-entry)]
                                  (.addEntry workbench new-entry)
                                  (recur 0 (update stats :moved-from-queues + (- new-entry-size entry-size))))))

                            (and front-too-small
                                 (zero? (flow-receiver/size ready-urls))
                                 (>= now (+ (sieve/last-flush sieve) constants/min-flush-interval)))
                            (do
                              (sieve/flush sieve)
                              (recur 0 stats))

                            (and front-too-small (pos? (flow-receiver/size ready-urls)))
                            (recur 0 (enlarge-front thread-data stats))

                            :else
                            (recur 0 stats)))

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
      (log/error :unexpected-ex {:ex t})))
  (log/info :distributor-thread :graceful-shutdown)
  true)
