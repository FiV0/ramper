(ns repl-sessions.utils)

(defmacro wrap-timer
  "Evaluates the `body` and returns the time it took."
  [& body]
  `(let [start# (System/currentTimeMillis)]
     ~@body
     (- (System/currentTimeMillis) start#)))
