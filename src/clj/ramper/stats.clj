(ns ramper.stats
  "A namespace for aggregating statistics across all threads of an agent."
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.frontier.workbench :as workbench]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.sieve.disk-flow-receiver :as flow-receiver]))

(def stats (atom {}))

(defn stats-loop [stats runtime-config {:keys [urls-crawled] :as _frontier} stats-chan]
  (async/go
    (try
      (while (not (runtime-config/stop? @runtime-config))
        (let [[new-stats ch] (async/alts! [stats-chan (async/timeout constants/stats-interval)])]
          (when  (= ch stats-chan)
            ;; TODO cleanup this mess
            (cond
              (contains? new-stats :fetching-thread/content-length)
              (swap! stats update :total-bytes (fnil + 0) (:fetching-thread/content-length new-stats))
              (contains? new-stats :fetching-thread/purge)
              (swap! stats update :visit-states-purged (fnil inc 0))
              (contains? new-stats :fetching-thread/sleep)
              (swap! stats update :fetching-threads/total-sleep (fnil + 0) (:fetching-thread/sleep new-stats))
              (contains? new-stats :parsing-thread/sleep)
              (swap! stats update :parsing-threads/total-sleep (fnil + 0) (:parsing-thread/sleep new-stats))
              :else
              (swap! stats merge new-stats {:urls-crawled @urls-crawled}))))
        #_(when-let [new-stats (async/<! stats-chan)]
            (swap! stats merge new-stats {:urls-crawled @urls-crawled})
            (async/<! (async/timeout constants/stats-interval))))
      (catch Throwable t
        (log/error :unexpected-ex {:ex t})))
    (log/info :graceful-shutdown {:type :stats-loop})
    true))


(defn stats-printing-loop [runtime-config frontier]
  (async/go-loop []
    (when (not (runtime-config/stop? @runtime-config))
      (println "todo-queue: " (-> frontier :todo-queue deref count))
      (println "done-queue: " (-> frontier :done-queue deref count))
      (println "refill-queue: " (-> frontier :refill-queue deref count))
      (println "ready-urls: " (-> frontier :ready-urls flow-receiver/size))
      (println "workbench size: " (-> frontier :workbench deref workbench/nb-workbench-entries))
      (async/<! (async/timeout 1000))
      (recur))))
