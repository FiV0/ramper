(ns ramper.stats
  "A namespace for aggregating statistics across all threads of an agent."
  (:require [clojure.core.async :as async]
            [io.pedestal.log :as log]
            [ramper.constants :as constants]
            [ramper.runtime-configuration :as runtime-config]))

(def stats (atom {}))

(defn stats-loop [stats runtime-config {:ramper/keys [urls-crawled] :as _frontier} stats-chan]
  (async/go
    (try
      (while (not (runtime-config/stop? @runtime-config))
        (let [[new-stats ch] (async/alts! [stats-chan (async/timeout constants/stats-interval)])]
          (when (= ch stats-chan)
            (swap! stats merge new-stats {:urls-crawled @urls-crawled}))))
      (catch Throwable t
        (log/error :unexpected-ex {:ex t})))
    (log/info :graceful-shutdown {:type :stats-thread})
    true))
