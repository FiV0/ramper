(ns repl-sessions.agent-testing
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [ramper.agent :as agent]
            [ramper.frontier :as frontier]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.stats :as stats]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]))

(defn reinit-runtime-config
  "For testing purposes only."
  ([] (reinit-runtime-config runtime-config/runtime-config))
  ([runtime-config]
   (let [root-dir (util/temp-dir "ramper-root")
         frontier-dir (io/file root-dir "frontier")
         store-dir (io/file root-dir "store")]
     (when-not (.exists frontier-dir)
       (.mkdirs frontier-dir))
     (when-not (.exists store-dir)
       (.mkdirs store-dir))
     (swap! runtime-config assoc
            :ramper/root-dir root-dir
            :ramper/frontier frontier-dir
            :ramper/store-dir store-dir))))

(def my-startup-config {:ramper/seed-file (io/resource "seed.txt")
                        :ramper/init-front-size 1000})

(swap! runtime-config/runtime-config merge my-startup-config)

(:ramper/seed-file @runtime-config/runtime-config)
(:ramper/is-new @runtime-config/runtime-config)

(runtime-config/reinit-runtime-config)

(def my-frontier (frontier/frontier @runtime-config/runtime-config))

(agent/start-stats-loop stats/stats
                        runtime-config/runtime-config
                        my-frontier
                        (async/chan))

(swap! runtime-config/runtime-config assoc :ramper/runtime-stop true)
(swap! runtime-config/runtime-config assoc :ramper/runtime-stop false)

(s/valid? ::runtime-config/runtime-config @runtime-config/runtime-config)
(s/explain ::runtime-config/runtime-config @runtime-config/runtime-config)

(def my-agent (agent/agent* runtime-config/runtime-config))
(agent/stop my-agent)

(thread-utils/get-threads "ramper.s")
(run! #(.stop %) (thread-utils/get-threads "ramper.s"))
