(ns ramper.frontier
  "The frontier is an atom that contains a map of datastructures shared across different
  threads of an agent.")

(def frontier (atom {}))
