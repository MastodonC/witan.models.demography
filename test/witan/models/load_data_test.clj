(ns witan.models.load-data-test
  (:require [witan.models.load-data :refer :all]
            [clojure.test :refer :all]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [witan.models.dem.ccm.schemas :refer [BirthsDataSchema]]))

;;Test data
(def test-file-1 "test_data/r_outputs_for_testing/load_data/bristol_births_data.csv") ;;clean data

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
