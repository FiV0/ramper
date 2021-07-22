(ns ramper.stats
  "A namespace for aggregating statistics across all threads of an agent.")

(def stats (atom {}))
