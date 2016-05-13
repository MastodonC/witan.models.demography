(ns witan.models.load-data-test
  (:require [witan.models.load-data :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [witan.models.dem.ccm.fert.hist-asfr-age :refer [BirthsDataSchema]]))

;;Test data
(def test-file-1 "resources/test_data/bristol_births_data.csv") ;;clean data

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

(deftest load-dataset-test
  (let [data-map (load-dataset :births-data test-file-1)
        data (:births-data data-map)]
    (testing "Loaded dataset has correct column names"
      (is (= [:gss-code :sex :age :births :year] (ds/column-names data))))
    (testing "Dataset was loaded"
      (is (every? identity (vals data-map))))
    (testing "Loaded dataset is a core.matrix dataset"
      (is (= clojure.core.matrix.impl.dataset.DataSet (type data))))
    (testing "Values correctly coerced"
      (is (= data (s/validate BirthsDataSchema data))))))
