(ns witan.models.load-data-test
  (:require [witan.models.load-data :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]))

(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

(deftest custom-keyword-test
  (testing "String column names are keywordised, with '.' and ' ' replaced by '-'."
    (is (= [:col-1 :col2 :col-3]
           (#'witan.models.load-data/custom-keyword ["col 1" "col2" "col.3"])))))

(def test-file "resources/test_data/bristol_births_data.csv")

(deftest load-csv-test
  (testing "Loading csv returns a map of column names vector and values vector."
    (is (map? (#'witan.models.load-data/load-csv test-file)))
    (is (= 2 (count (#'witan.models.load-data/load-csv test-file))))
    (is (= '(:column-names :columns)
           (keys (#'witan.models.load-data/load-csv test-file))))
    (is (= [:gss-code :sex :age :births :year]
           (:column-names (#'witan.models.load-data/load-csv test-file))))
    (is (= [["E06000023" "F" "15" "7.768" "2013"] ["E06000023" "F" "16" "17.746" "2013"]
            ["E06000023" "F" "17" "43.592" "2013"] ["E06000023" "F" "18" "75.838" "2013"]
            ["E06000023" "F" "19" "99.275" "2013"] ["E06000023" "F" "20" "121.681" "2013"]
            ["E06000023" "F" "21" "185.04" "2013"] ["E06000023" "F" "22" "216.782" "2013"]
            ["E06000023" "F" "23" "245.872" "2013"] ["E06000023" "F" "24" "267.65" "2013"]
            ["E06000023" "F" "25" "295.071" "2013"] ["E06000023" "F" "26" "296.78" "2013"]
            ["E06000023" "F" "27" "324.477" "2013"] ["E06000023" "F" "28" "365.32" "2013"]
            ["E06000023" "F" "29" "399.628" "2013"] ["E06000023" "F" "30" "435.648" "2013"]
            ["E06000023" "F" "31" "469.04" "2013"] ["E06000023" "F" "32" "477.409" "2013"]
            ["E06000023" "F" "33" "436.87" "2013"] ["E06000023" "F" "34" "405.4" "2013"]
            ["E06000023" "F" "35" "324.836" "2013"] ["E06000023" "F" "36" "271.315" "2013"]
            ["E06000023" "F" "37" "227.466" "2013"] ["E06000023" "F" "38" "188.331" "2013"]
            ["E06000023" "F" "39" "136.926" "2013"] ["E06000023" "F" "40" "95.694" "2013"]
            ["E06000023" "F" "41" "76.079" "2013"] ["E06000023" "F" "42" "43.602" "2013"]
            ["E06000023" "F" "43" "24.278" "2013"] ["E06000023" "F" "44" "12.482" "2013"]]
           (:columns (#'witan.models.load-data/load-csv test-file))))))

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

(deftest make-ordered-ds-schema-test
  (testing "Column schema generated from vector"
    (is (= ColumnsSchema (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year s/Int]
                                                  [:gss-code s/Str] [:births s/Num]]))))) 
(deftest make-row-schema-test
  (testing "Generate row shema from column schema"
    (is (= RowSchema (make-row-schema ColumnsSchema)))))
