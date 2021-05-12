(ns terminus.core-test
  (:require
   [clojure.test :refer [deftest testing is]]
   [terminus.core :as terminus]))

(deftest query-translating
  (testing "Can parse queries from datalog to WOQL"
    (is (= (terminus/parse-query
             '[:find ?x ?y
               :in $ ?foo
               :where
               [?x :thing ?y]
               [?y :beep ?foo]])
           {:type :find
            :vars '(?x ?y)
            :in '($ ?foo)
            :query '([?x :thing ?y]
                     [?y :beep ?foo])}))

    (is (= (terminus/parse-query
             '[:find ?x ?y
               :where
               [?x :thing ?y]
               [?y :beep ?foo]])
           {:type :find
            :vars '(?x ?y)
            :in '()
            :query '([?x :thing ?y]
                     [?y :beep ?foo])})))
  )
