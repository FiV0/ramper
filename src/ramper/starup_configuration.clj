(ns ramper.startup-configuration
  "Read and check a startup-configuration."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]))

(s/def ::supported-keys (s/keys :req [:ramper/dns-threads
                                      :ramper/fetching-threads
                                      :ramper/parsing-threads
                                      :ramper/max-urls
                                      :ramper/max-urls-per-scheme-authority
                                      :ramper/root-dir
                                      :ramper/seed-file
                                      :ramper/user-agent
                                      :ramper/user-agent-from
                                      :ramper/ip-delay
                                      :ramper/ip-delay-scheme-authority]))

(defn read-config
  "Read the config from `file` or from a default config."
  ([] (read-config (io/file (io/resource "ramper-config.edn"))))
  ([file]
   (edn/read-string (slurp (io/file file)))))

(s/fdef read-config
  :ret ::supported-keys)

(comment
  (read-config)

  )
