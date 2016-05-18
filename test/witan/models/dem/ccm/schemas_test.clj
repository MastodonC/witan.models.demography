(ns witan.models.dem.ccm.schemas-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [witan.models.dem.ccm.schemas :refer :all]))

(def ColumnsSchema
  {:column-names [(s/one (s/eq :age) ":age")
                  (s/one (s/eq :sex) ":sex")
                  (s/one (s/eq :year) ":year")
                  (s/one (s/eq :gss-code) ":gss-code")
                  (s/one (s/eq :births) ":births")]
   :columns [(s/one [s/Int] "col age")
             (s/one [s/Str] "col sex")
             (s/one [s/Int] "col year") 
             (s/one [s/Str] "col gss-code")
             (s/one [s/Num] "col births")]
   s/Keyword s/Any})

(def RowSchema
  [(s/one s/Int "col age")
   (s/one s/Str "col sex")
   (s/one s/Int "col year")
   (s/one s/Str "col gss-code")
   (s/one s/Num "col births")])

;;Tests
(deftest make-ordered-ds-schema-test
  (testing "Column schema generated from vector"
    (is (= ColumnsSchema (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year s/Int]
                                                  [:gss-code s/Str] [:births s/Num]]))))) 
(deftest make-row-schema-test
  (testing "Generate row shema from column schema"
    (is (= RowSchema (make-row-schema ColumnsSchema)))))
