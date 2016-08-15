(ns witan.models.run-models-test
  (:require [witan.models.run-models :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.datasets :as wds]
            [witan.models.load-data :as ld]
            [witan.models.dem.ccm.core.projection-loop :as core]
            [witan.models.dem.ccm.core.projection-loop-test :as core-test]
            [witan.models.dem.ccm.fert.fertility :as fert]
            [witan.models.dem.ccm.mort.mortality :as mort]
            [witan.models.dem.ccm.mig.migration :as mig]
            [witan.models.dem.ccm.components-functions :as cf]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;; Load testing data
(def datasets  {:historic-births-by-age-mother
                "./datasets/default_datasets/fertility/historic_births_by_age_of_mother.csv"
                :historic-births
                "./datasets/default_datasets/fertility/historic_births.csv"
                :historic-population
                "./datasets/default_datasets/core/historic_population.csv"
                :historic-deaths
                "./datasets/default_datasets/mortality/historic_deaths.csv"
                :domestic-in-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_domestic_in.csv"
                :domestic-out-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_domestic_out.csv"
                :international-in-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_international_in.csv"
                :international-out-migrants
                "./datasets/default_datasets/migration/historic_migration_flows_international_out.csv"
                :future-mortality-trend-assumption
                "./datasets/default_datasets/mortality/future_mortality_trend_assumption.csv"})

(def gss-bristol "E06000023")

(def params-2015 {;; Core module
                  :first-proj-year 2014
                  :last-proj-year 2015
                  ;; Fertility module
                  :fert-base-year 2014
                  :proportion-male-newborns (double (/ 105 205))
                  ;; Mortality module
                  :start-year-avg-mort 2010
                  :end-year-avg-mort 2014
                  ;; Migration module
                  :start-year-avg-domin-mig 2003
                  :end-year-avg-domin-mig 2014
                  :start-year-avg-domout-mig 2003
                  :end-year-avg-domout-mig 2014
                  :start-year-avg-intin-mig 2003
                  :end-year-avg-intin-mig 2014
                  :start-year-avg-intout-mig 2003
                  :end-year-avg-intout-mig 2014})

(deftest get-dataset-test
  (testing "The data is filtered on local authority."
    (let [dataset (:historic-births-by-age-mother
                   (get-dataset
                    :historic-births-by-age-mother
                    "./datasets/default_datasets/fertility/historic_births_by_age_of_mother.csv"
                    "E06000023"))
          gss-dataset (i/$ :gss-code dataset)]
      (is (true?
           (every? #(= % "E06000023") gss-dataset))))))

(def r-output-2015 (ld/load-datasets
                    {:end-population
                     "./datasets/test_datasets/r_outputs_for_testing/core/bristol_end_population_2015.csv"}))

(deftest run-workspace-test
  (testing "The historical and projection data is returned"
    (let [proj-bristol-2015 (wds/select-from-ds (run-workspace datasets gss-bristol params-2015)
                                                {:year 2015})
          r-proj-bristol-2015 (ds/rename-columns (:end-population r-output-2015)
                                                 {:popn :popn-r})
          joined-ds (wds/join proj-bristol-2015 r-proj-bristol-2015 [:gss-code :sex :age :year])]
      (is proj-bristol-2015)
      (is (= (:shape proj-bristol-2015) (:shape r-proj-bristol-2015)))
      (is (every? #(fp-equals? (wds/subset-ds joined-ds :rows % :cols :popn)
                               (wds/subset-ds joined-ds :rows % :cols :popn-r) 0.0000000001)
                  (range (first (:shape joined-ds))))))))

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
      (is (every? #(= (wds/subset-ds joined-data :rows % :cols :district)
                      (wds/subset-ds joined-data :rows % :cols :district-out))
                  (range (first (:shape joined-data))))))))
