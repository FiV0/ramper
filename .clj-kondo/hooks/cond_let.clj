(ns hooks.cond-let
  (:require [clj-kondo.hooks-api :as api]))

(defn cond-let [{:keys [node]}]
  (let [clauses (rest (:children node))
        metadata (meta node)]
    (cond-> (if (empty? clauses)
              {:node (api/token-node nil)}
              (let [[test-form expr-form & clauses] (rest (:children node))
                    clauses-node (with-meta
                                   (api/list-node
                                    (list*
                                     (api/token-node 'cond-let)
                                     clauses))
                                   metadata)]
                (when-not (and test-form expr-form)
                  (throw (ex-info "cond-let requires an even number of forms" {})))
                (if (vector? (api/sexpr test-form))
                  (let [[sym val & rest] (:children test-form)]
                    (when-not (and sym val (empty? rest))
                      (throw (ex-info "cond-let binding expr requires exactly two forms" {})))
                    {:node (api/list-node
                            (list
                             (api/token-node 'if-let)
                             (api/vector-node [sym val])
                             expr-form
                             clauses-node))})
                  {:node (api/list-node
                          (list
                           (api/token-node 'if)
                           test-form
                           expr-form
                           clauses-node))})))
      metadata (with-meta (meta node)))))
