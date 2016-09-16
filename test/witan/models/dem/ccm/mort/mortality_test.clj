(ns witan.models.dem.ccm.mort.mortality-test
  (:require [witan.models.dem.ccm.mort.mortality :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.core.projection-loop-test :as plt]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;Test inputs
;;historic-deaths = deaths at start of R code
;;historic-births = historic.mort.age0 in R code
;;historic-popn = MYE-Est in R code
(def hist-asmr-inputs
  (ld/load-datasets {:historic-deaths
                     "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                     :historic-births
                     "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                     :historic-population
                     "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"
                     :population-at-risk ;;this actually comes from the proj loop but for test use this csv
                     "./datasets/test_datasets/r_outputs_for_testing/core/bristol_popn_at_risk_2015.csv"
                     :future-mortality-trend-assumption
                     "./datasets/test_datasets/model_inputs/mort/future_mortality_trend_assumption.csv"}))

(def proj-death-inputs
  (assoc (ld/load-datasets {:population-at-risk ;;this actually comes from the proj loop but for test use this csv
                            "./datasets/test_datasets/r_outputs_for_testing/core/bristol_popn_at_risk_2015.csv"})
         :loop-year 2015))

;;Output from R calc-historic-asmr function for comparison
(def historic-asmr-r (:historic-asmr (ld/load-dataset
                                      :historic-asmr
                                      "./datasets/test_datasets/r_outputs_for_testing/mort/bristol_historic_asmr.csv")))

(def proj-deaths-fixed-r (-> :deaths
                             (ld/load-dataset "./datasets/test_datasets/r_outputs_for_testing/mort/bristol_mortality_module_r_output_2015.csv")
                             :deaths
                             (ds/rename-columns {:deaths :deaths-r})))

(def proj-deaths-national-trend-r (-> :deaths
                                      (ld/load-dataset "./datasets/test_datasets/r_outputs_for_testing/mort/bristol_mortality_module_r_output_2015.csv")
                                      :deaths
                                      (ds/rename-columns {:deaths :deaths-r})))

(def proj-asmr-avg-applynationaltrend-r
  (-> :projected-asmr
      (ld/load-dataset "./datasets/test_datasets/r_outputs_for_testing/mort/bristol_projected_asmr_avg_applynationaltrend_r_output_2015_2017.csv")
      :projected-asmr
      (ds/rename-columns {:death-rate :death-rate-r})))

(def params {:start-year-avg-mort 2010
             :end-year-avg-mort 2014
             :first-proj-year 2015
             :last-proj-year 2017
             :mort-scenario :principal
             :mort-variant :average-fixed})

(deftest calc-historic-asmr-test
  (testing "Death rates are calculated correctly."
    (let [hist-asmr-r (ds/rename-columns historic-asmr-r {:death-rate :death-rate-r})
          joined-asmr (-> hist-asmr-inputs
                          calc-historic-asmr
                          :historic-asmr
                          (wds/join hist-asmr-r [:gss-code :sex :age :year]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-asmr :rows % :cols :death-rate-r)
                               (wds/subset-ds joined-asmr :rows % :cols :death-rate)
                               0.0000000001)
                  (range (first (:shape joined-asmr))))))))

(deftest project-asmr-average-applynationaltrend-test
  (testing "mortality rates projected correctly"
    (let [projected-asmr-clj (-> hist-asmr-inputs
                                 calc-historic-asmr
                                 (project-asmr-1-1-0
                                  (assoc params :mort-variant :average-applynationaltrend))
                                 :initial-projected-mortality-rates)
          joined-proj-asmr (wds/join proj-asmr-avg-applynationaltrend-r
                                     projected-asmr-clj
                                     [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (wds/subset-ds joined-proj-asmr :rows % :cols :death-rate)
                               (wds/subset-ds joined-proj-asmr :rows % :cols :death-rate-r)
                               0.0000001)
                  (range (first (:shape joined-proj-asmr))))))))

(deftest project-deaths-from-fixed-rates-test
  (let [proj-deaths-clj (-> hist-asmr-inputs
                            calc-historic-asmr
                            (project-asmr-1-1-0 params)
                            (assoc :loop-year 2017)
                            project-deaths-1-1-0
                            :deaths)
        joined-proj-deaths (wds/join proj-deaths-fixed-r proj-deaths-clj
                                     [:gss-code :sex :age :year])]
    (is (every? #(fp-equals? (wds/subset-ds joined-proj-deaths :rows % :cols :deaths-r)
                             (wds/subset-ds joined-proj-deaths :rows % :cols :deaths)
                             0.0000001)
                (range (first (:shape joined-proj-deaths)))))))

(deftest project-deaths-1-1-0-test
  (let [death-rates (-> hist-asmr-inputs
                        calc-historic-asmr
                        (project-asmr-1-1-0 (assoc params :mort-variant :average-applynationaltrend)))
        proj-deaths-clj (-> death-rates
                            (merge proj-death-inputs)
                            project-deaths-1-1-0
                            :deaths)
        joined-proj-deaths (wds/join proj-deaths-national-trend-r proj-deaths-clj
                                     [:gss-code :sex :age :year])]
    (is (every? #(fp-equals? (wds/subset-ds joined-proj-deaths :rows % :cols :deaths-r)
                             (wds/subset-ds joined-proj-deaths :rows % :cols :deaths)
                             0.0000001)
                (range (first (:shape joined-proj-deaths)))))))
