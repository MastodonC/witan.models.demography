(ns witan.models.dem.ccm.fert.fertility-test
  (:require [witan.models.dem.ccm.fert.fertility :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;;;;;;;;;;;;;;;;
;; Test inputs ;;
;;;;;;;;;;;;;;;;;

(def fertility-inputs (ld/load-datasets
                       {:historic-births-by-age-mother
                        "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                        :historic-births
                        "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                        :historic-population
                        "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"
                        :population-at-risk ;;this actually comes from the proj loop but for test use this csv
                        "./datasets/test_datasets/r_outputs_for_testing/core/bristol_popn_at_risk_2015.csv"
                        :future-fertility-trend-assumption
                        "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"}))

(def params {:variant :fixed
             :first-proj-year 2015
             :last-proj-year 2016
             :fert-scenario :principal-2012
             :fert-base-year 2014
             :proportion-male-newborns (double (/ 105 205))})

(def params-applynationaltrend {:variant :applynationaltrend
                                :first-proj-year 2015
                                :last-proj-year 2018
                                :fert-scenario :principal-2012
                                :fert-base-year 2014
                                :proportion-male-newborns (double (/ 105 205))})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; R outputs for comparison ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def fertility-outputs-r (ld/load-datasets
                          {:projected-asfr-finalyearfixed
                           "./datasets/test_datasets/r_outputs_for_testing/fert/bristol_initial_proj_asfr_finalyrfixed_2015.csv"
                           :projected-asfr-finalyearapplynationaltrend
                           "./datasets/test_datasets/r_outputs_for_testing/fert/bristol_initial_proj_asfr_finalyrapplynationaltrend_2015_2018.csv"
                           :births
                           "./datasets/test_datasets/r_outputs_for_testing/fert/bristol_fertility_module_r_output_2015.csv"
                           :historic-asfr
                           "./datasets/test_datasets/r_outputs_for_testing/fert/bristol_historic_asfr.csv"
                           :births-by-age-sex-mother
                           "./datasets/test_datasets/r_outputs_for_testing/fert/bristol_proj_births_age_sex_mother_2015.csv"}))



(deftest calculate-historic-asfr-test
  (testing "Historic ASFR calculation"
    (let [calc-hist-asfr (:historic-asfr
                          (calculate-historic-asfr fertility-inputs params))
          r-output-hist-asfr (ds/rename-columns
                              (:historic-asfr fertility-outputs-r)
                              {:fert-rate :fert-rate-r})
          joined-hist-asfr (wds/join calc-hist-asfr r-output-hist-asfr
                                     [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (wds/subset-ds joined-hist-asfr :rows % :cols :fert-rate)
                               (wds/subset-ds joined-hist-asfr :rows % :cols :fert-rate-r)
                               0.0000000001)
                  (range (first (:shape joined-hist-asfr))))))))

(deftest project-asfr-1-0-0-test
  (testing "Fertility rates are projected correctly."
    (let [proj-asfr-r (-> fertility-outputs-r
                          :projected-asfr-finalyearfixed
                          (ds/rename-columns {:fert-rate :fert-rate-r}))
          joined-asfr (-> fertility-inputs
                          (calculate-historic-asfr params)
                          project-asfr-1-0-0
                          :initial-projected-fertility-rates
                          (wds/join proj-asfr-r [:gss-code :sex :age]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-asfr :rows % :cols :fert-rate-r)
                               (wds/subset-ds joined-asfr :rows % :cols :fert-rate)
                               0.0000000001)
                  (range (first (:shape joined-asfr))))))))

(deftest project-asfr-1-1-0-test
  (testing "Fertility rates are projected correctly for fixed variant."
    (let [n (wds/row-count (:projected-asfr-finalyearfixed fertility-outputs-r))
          proj-asfr-r (-> fertility-outputs-r
                          :projected-asfr-finalyearfixed
                          (ds/rename-columns {:fert-rate :fert-rate-r})
                          (ds/add-column :year (repeat n 2015))
                          (ds/select-columns [:gss-code :sex :age :year :fert-rate-r]))
          joined-asfr (-> fertility-inputs
                          (calculate-historic-asfr params)
                          (project-asfr-1-1-0 params)
                          :initial-projected-fertility-rates
                          (wds/join proj-asfr-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-asfr :rows % :cols :fert-rate-r)
                               (wds/subset-ds joined-asfr :rows % :cols :fert-rate)
                               0.0000000001)
                  (range (first (:shape joined-asfr)))))))
  (testing "Fertility rates match R output for applynationaltrend variant."
    (let [proj-asfr-r (-> fertility-outputs-r
                          :projected-asfr-finalyearapplynationaltrend
                          (ds/rename-columns {:fert-rate :fert-rate-r})
                          (ds/select-columns [:gss-code :sex :age :year :fert-rate-r]))
          init-asfr (-> fertility-inputs
                        (calculate-historic-asfr params-applynationaltrend)
                        (project-asfr-1-1-0 params-applynationaltrend)
                        :initial-projected-fertility-rates)
          joined-asfr (-> fertility-inputs
                          (calculate-historic-asfr params-applynationaltrend)
                          (project-asfr-1-1-0 params-applynationaltrend)
                          :initial-projected-fertility-rates
                          (wds/join proj-asfr-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-asfr :rows % :cols :fert-rate-r)
                               (wds/subset-ds joined-asfr :rows % :cols :fert-rate)
                               0.0000000001)
                  (range (first (:shape joined-asfr))))))))

(deftest project-births-1-0-0-test
  (testing "Projected births match R values"
    (let [proj-births-r (-> fertility-outputs-r
                            :births-by-age-sex-mother
                            (ds/rename-columns {:births :births-r}))
          joined-births (-> fertility-inputs
                            (calculate-historic-asfr params)
                            project-asfr-1-0-0
                            project-births-1-0-0
                            :births-by-age-sex-mother
                            (wds/join proj-births-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-births :rows % :cols :births-r)
                               (wds/subset-ds joined-births :rows % :cols :births)
                               0.00001)
                  (range (first (:shape joined-births))))))))

(deftest project-births-1-1-0-test
  (testing "Projected births match R values using fixed ASFR projection"
    (let [proj-births-r (-> fertility-outputs-r
                            :births-by-age-sex-mother
                            (ds/rename-columns {:births :births-r}))
          joined-births (-> fertility-inputs
                            (assoc :loop-year 2015)
                            (calculate-historic-asfr params)
                            (project-asfr-1-1-0 params)
                            (project-births-1-1-0 params)
                            :births-by-age-sex-mother
                            (wds/join proj-births-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-births :rows % :cols :births-r)
                               (wds/subset-ds joined-births :rows % :cols :births)
                               0.00001)
                  (range (first (:shape joined-births))))))))

(deftest combine-into-births-by-sex-test
  (testing "Births by sex have been gathered correctly & match R output"
    (let [births-by-sex-r (-> fertility-outputs-r
                              :births
                              (ds/rename-columns {:births :births-r}))
          joined-births-by-sex (-> fertility-inputs
                                   (calculate-historic-asfr params)
                                   project-asfr-1-0-0
                                   project-births-1-0-0
                                   (combine-into-births-by-sex params)
                                   :births
                                   (wds/join births-by-sex-r [:gss-code :sex]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-births-by-sex :rows % :cols :births-r)
                               (wds/subset-ds joined-births-by-sex :rows % :cols :births)
                               0.00000001)
                  (range (first (:shape joined-births-by-sex))))))))
