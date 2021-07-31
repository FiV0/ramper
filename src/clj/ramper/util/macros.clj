(ns ramper.util.macros
  "A small set of utility macros.")

;; Copied from core.clj and modified.
;; TODO think about if vector differentiation is actually a good idea
(defmacro cond-let
  "Takes a set of test/expr pairs. A test expr can either be a usual
  test expr as per `clojure.core/cond` or a vector of two forms. In the
  latter case the the second form of the vector is evaluated and
  bound to the first form. It evaluates each test or binding form
  at a time. If a test or binding form returns logical true,
  cond-let evaluates and returns the value of the corresponding expr
  and doesn't evaluate any of the other tests or exprs.
  (cond-let) returns nil."
  [& clauses]
  (when clauses
    (let [first-clause (first clauses)]
      (list (if (vector? first-clause)
              (do
                (when-not (= 2 (count first-clause))
                  (throw (IllegalArgumentException.
                          "cond-let binding expr requires exactly two forms")))
                'if-let)
              'if)
            first-clause
            (if (next clauses)
              (second clauses)
              (throw (IllegalArgumentException.
                      "cond-let requires an even number of forms")))
            (cons 'cond-let (nnext clauses))))))

(comment
  (loop [l (apply list (range 10))]
    (cond-let [e (peek l)]
              (do (println "got element" e)
                  (recur (pop l)))


              (= 5 (count l))
              (do (println "emptying l")
                  (recur '()))

              :else
              (println "we are done")))

  (cond-let)
  )
