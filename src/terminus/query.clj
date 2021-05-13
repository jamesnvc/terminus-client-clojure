(ns terminus.query
  (:require
   [clojure.core.match :refer [match]]))

(defn parse-query
  "Take a query as clojure data & convert it to WOQL.
  https://terminusdb.com/docs/terminushub/reference/server/woql/"
  [q]
  (let [type (first q)
        [vars more] (split-with (complement keyword?) (rest q))
        [ins [_ & query]]
        (if (= :in (first more))
          (split-with (complement keyword?) (rest more))
          [[] more])]
    {:type type
     :vars vars
     :in ins
     :query query}))

(def indexed (partial map-indexed vector))

(defn query-elt->woql
  [q]
  (match q
    [e a v] {"@type" "woql:Triple"
             "woql:subject" (query-elt->woql e)
             "woql:predicate" (query-elt->woql a)
             "woql:object" (query-elt->woql v)}

    (x :guard symbol?) {"@type" "woql:Variable"
                        "woql:variable_name" {"@value" x
                                              "@type" "TODO"}}
    (k :guard keyword?) {"@type" "woql:Node"
                         ;; [TODO] namespace for this?
                         "woql:node" k}))

(defn query->woql
  [{:keys [vars in query]}]
  {"@type" "woql:Select"
   "woql:variable_list" (for [v vars]
                          {"@type" "woql:VariableListElement"
                           "woql:variable_name"
                           {"@value" v
                            ;; [TODO] get from schema?
                            #_#_"@type" "xsd:string"}})
   "woql:query" {"@type" "woql:And"
                 "woql:query_list"
                 (for [[i q] query]
                   {"@type" "woql:QueryListElement"
                    "woql:index" {"@type" "xsd:nonNegativeInteger"
                                  "@value" i}
                    "woql:query" (query-elt->woql q)})
                 }}
  )

(comment

  (parse-query
    '[:find ?x ?y
      :in $ ?foo
      :where
      [?x :thing ?y]
      [?y :beep ?foo]])

  (query-elt->woql '[?x :thing ?y])

  )
