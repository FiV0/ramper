(ns repl-sessions.agent-testing
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [lambdaisland.uri :as uri]
            [ramper.agent :as agent]
            [ramper.frontier :as frontier]
            [ramper.startup-configuration :as startup-config]
            [ramper.runtime-configuration :as runtime-config]
            [ramper.stats :as stats]
            [ramper.util :as util]
            [ramper.util.thread :as thread-utils]))

(defn reinit-runtime-config
  "For testing purposes only."
  ([] (reinit-runtime-config runtime-config/runtime-config))
  ([runtime-config]
   (let [{:ramper/keys [root-dir]} @runtime-config
         root-dir (or root-dir (util/temp-dir "ramper-root"))
         frontier-dir (io/file root-dir "frontier")
         store-dir (io/file root-dir "store")]
     (when-not (.exists frontier-dir)
       (.mkdirs frontier-dir))
     (when-not (.exists store-dir)
       (.mkdirs store-dir))
     (swap! runtime-config assoc
            :ramper/root-dir root-dir
            :ramper/frontier-dir frontier-dir
            :ramper/store-dir store-dir
            :ramper/runtime-stop false))))

(def my-startup-config {:ramper/seed-urls (startup-config/read-urls* (io/file (io/resource "seed.txt")))
                        :ramper/init-front-size 1000})

(def my-local-startup-config {:ramper/seed-urls (list (uri/uri "http://localhost:8080"))
                              :ramper/init-front-size 1000})

(def my-runtime-config (atom {:ramper/aux-buffer-size               (* 64 1024)
                              :ramper/cookies-max-byte-size         2048
                              :ramper/dns-threads                   50
                              :ramper/fetching-threads              512
                              :ramper/ip-delay                      2000 ;2 seconds
                              ;; :ramper/ip-delay                      0
                              :ramper/is-new                        true
                              :ramper/keepalive-time                5000
                              :ramper/max-urls                      10000
                              :ramper/max-urls-per-scheme+authority 10001
                              ;; Current estimation of the size of the front in ip addresses. Adaptively
                              ;; increased by the fetching threads whenever they have to wait to retrieve
                              ;; a visit state from the todo queue.
                              :ramper/parsing-threads               64
                              :ramper/required-front-size           1000
                              :ramper/runtime-pause                 false
                              :ramper/runtime-stop                  false
                              :ramper/scheme+authority-delay        2000 ;2 seconds
                              ;; :ramper/scheme+authority-delay        0
                              :ramper/sieve-size                    (* 64 1024)
                              :ramper/store-buffer-size             (* 64 1024)
                              :ramper/url-cache-max-byte-size       (* 1024 1024 1024)
                              :ramper/user-agent                    "ramper"
                              :ramper/workbench-max-byte-size       (* 512 1024 1024)
                              :ramper/root-dir (util/make-absolute "test-crawl")}))

(swap! my-runtime-config merge my-startup-config)
(swap! my-runtime-config merge my-local-startup-config)

(swap! my-runtime-config assoc :ramper/runtime-stop true)
(swap! my-runtime-config assoc :ramper/runtime-stop false)

(s/valid? ::runtime-config/runtime-config @my-runtime-config)
(s/explain ::runtime-config/runtime-config @my-runtime-config)

(reinit-runtime-config my-runtime-config)

(comment
  (reset! stats/stats {})
  (deref stats/stats)

  (def my-agent (agent/agent* my-runtime-config))
  (agent/stop my-agent)

  (require '[ramper.frontier.workbench :as workbench]
           '[ramper.frontier.workbench.visit-state :as visit-state]
           '[ramper.frontier.workbench.virtualizer :as virtual]
           '[ramper.util.url :as url]
           '[ramper.store :as store]
           '[ramper.store.simple-store :as simple-store])

  (let [reader (simple-store/simple-store-reader (-> my-runtime-config deref :ramper/store-dir))]
    (loop [res []]
      (if-let [rec (store/read reader)]
        (recur (conj res rec))
        (println (str "contains " (count res) " records")))))

  (-> my-agent :frontier )

  (-> my-agent :frontier :scheme+authority-to-count)
  (-> my-agent :frontier :urls-crawled deref)
  (-> my-agent :frontier :url-cache (.cache))
  (-> my-agent :frontier :sieve .close)
  (-> my-agent :frontier :workbench deref workbench/nb-workbench-entries)
  (-> my-agent :frontier :workbench (workbench/scheme+authority-present? (url/scheme+authority "http://localhost:8080")))

  (run! #(.stop %) (thread-utils/get-threads "ramper.s"))

  )
