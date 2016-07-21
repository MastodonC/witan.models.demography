(ns witan.models.run-models-test
  (:require [witan.models.run-models :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.core.projection-loop :as core]
            [witan.models.dem.ccm.core.projection-loop-test :as core-test]
            [witan.models.dem.ccm.fert.fertility-mvp :as fert]
            [witan.models.dem.ccm.mort.mortality-mvp :as mort]
            [witan.models.dem.ccm.mig.net-migration :as mig]
            [witan.models.dem.ccm.components-functions :as cf]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;; Load testing data
(def datasets  {:ons-proj-births-by-age-mother
                "./datasets/default_datasets/fertility/proj-births-by-age-mother.csv"
                :historic-births
                "./datasets/default_datasets/fertility/historic_births.csv"
                :historic-population
                "./datasets/default_datasets/core/historic-population.csv"
                :historic-deaths
                "./datasets/default_datasets/mortality/historic_deaths.csv"
                :domestic-in-migrants
                "./datasets/default_datasets/migration/domestic_in_migrants.csv"
                :domestic-out-migrants
                "./datasets/default_datasets/migration/domestic_out_migrants.csv"
                :international-in-migrants
                "./datasets/default_datasets/migration/international_in_migrants.csv"
                :international-out-migrants
                "./datasets/default_datasets/migration/international_out_migrants.csv"})

(def gss-bristol "E06000023")

(def params-2015 core-test/params)

(def params-2040 core-test/params-2040)

(deftest get-dataset-test
  (testing "The data is filtered on local authority."
    (let [dataset (:ons-proj-births-by-age-mother
                   (get-dataset
                    :ons-proj-births-by-age-mother
                    "./datasets/default_datasets/fertility/proj-births-by-age-mother.csv"
                    "E06000023"))
          gss-dataset (i/$ :gss-code dataset)]
      (is (true?
           (every? #(= % "E06000023") gss-dataset))))))

(deftest run-ccm-test
  (testing "The historical and projection data is returned"
    (let [proj-core-2015 (ds/rename-columns (i/query-dataset
                                             (core/looping-test core-test/data-inputs params-2015)
                                             {:year {:$eq 2015}})
                                            {:popn :popn-core})
          proj-bristol-2015 (i/query-dataset (run-ccm datasets gss-bristol params-2015)
                                             {:year {:$eq 2015}})
          proj-core-2040 (ds/rename-columns (i/query-dataset
                                             (core/looping-test core-test/data-inputs params-2040)
                                             {:year {:$eq 2040}})
                                            {:popn :popn-core})
          proj-bristol-2040 (i/query-dataset (run-ccm datasets gss-bristol params-2040)
                                             {:year {:$eq 2040}})
          joined-proj-2015 (wds/join proj-bristol-2015 proj-core-2015 [:gss-code :sex :age :year])
          joined-proj-2040 (wds/join proj-bristol-2040 proj-core-2040 [:gss-code :sex :age :year])]
      ;; Compare projection values for 2015:
      (is (every? #(fp-equals? (i/sel joined-proj-2015 :rows % :cols :popn)
                               (i/sel joined-proj-2015 :rows % :cols :popn-core) 0.0001)
                  (range (first (:shape joined-proj-2015)))))
      ;; Compare projection values for 2040:
      (is (every? #(fp-equals? (i/sel joined-proj-2040 :rows % :cols :popn)
                               (i/sel joined-proj-2040 :rows % :cols :popn-core) 0.0001)
                  (range (first (:shape joined-proj-2040))))))))

(deftest get-district-test
  (testing "fn recovers correct district name"
    (is (= (get-district "E06000023") "Bristol, City of"))))

(def input-data-set (ds/dataset [{:year 2014 :gss-code "E06000023"}
                                 {:year 2015 :gss-code "E06000023"}]))

(def output-data-set (ds/dataset [{:year 2014 :gss-code "E06000023" :district-out "Bristol, City of"}
                                  {:year 2015 :gss-code "E06000023" :district-out "Bristol, City of"}]))

(deftest add-district-to-dataset-per-user-input-test
  (testing "district column added to dataset and populated"
    (let [district-added (add-district-to-dataset-per-user-input input-data-set "E06000023")
          joined-data (wds/join district-added output-data-set [:gss-code :year])]
      (is (every? #(= (i/sel joined-data :rows % :cols :district)
                      (i/sel joined-data :rows % :cols :district-out))
                  (range (first (:shape joined-data))))))))