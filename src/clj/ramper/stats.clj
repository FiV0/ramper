(ns ramper.stats
  "A namespace for aggregating statistics across all threads of an agent."
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.runtime-configuration :as runtime-config]))

(def stats (atom {}))

(defn stats-loop [stats runtime-config stats-chan]
  (async/go
    (try
      (while (not (runtime-config/stop? @runtime-config))
        (when-let [new-stats (async/<! stats-chan)]
          (swap! stats merge new-stats)))
      (catch Throwable t
        (log/error :unexpected-ex {:ex t})))
    (log/info :graceful-shutdown {:type :stats-thread})
    true))
