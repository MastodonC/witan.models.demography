(ns witan.models.dem.ccm.mig.migration-test
  (:require [witan.models.dem.ccm.mig.migration :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.workspace-api :refer [merge->]]
            [witan.models.dem.ccm.core.projection-loop :as loop]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(def data-inputs (ld/load-datasets
                  {:historic-births-by-age-mother
                   "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                   :historic-births
                   "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                   :historic-population
                   "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"
                   :historic-deaths
                   "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                   :domestic-in-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                   :domestic-out-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                   :international-in-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                   :international-out-migrants
                   "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                   :future-mortality-trend-assumption
                   "./datasets/test_datasets/model_inputs/mort/death_improvement.csv"
                   :future-fertility-trend-assumption
                   "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"}))

(def net-migration-r (:net-migration (ld/load-dataset
                                      :net-migration
                                      "./datasets/test_datasets/r_outputs_for_testing/mig/bristol_migration_module_r_output_2015.csv")))

(def params {:start-year-avg-domin-mig 2003
             :end-year-avg-domin-mig 2014
             :start-year-avg-domout-mig 2003
             :end-year-avg-domout-mig 2014
             :start-year-avg-intin-mig 2003
             :end-year-avg-intin-mig 2014
             :start-year-avg-intout-mig 2003
             :end-year-avg-intout-mig 2014
             :first-proj-year 2015
             :last-proj-year 2016
             :proportion-male-newborns (double (/ 105 205))
             :proj-asfr-variant :fixed
             :future-fert-scenario :principal-2012
             :fert-base-year 2014
             :start-year-avg-mort 2010
             :end-year-avg-mort 2014
             :proj-asmr-variant :average-fixed
             :mort-scenario :principal})

(def prepared-inputs (loop/prepare-inputs-1-0-0 data-inputs params))

(deftest combine-into-net-flows-test
  (testing "The net migration flows are calculated correctly."
    (let [net-mig-r (ds/rename-columns net-migration-r {:net-mig :net-mig-r})
          joined-mig (-> (merge-> data-inputs
                                  (projected-domestic-in-migrants params)
                                  (projected-domestic-out-migrants params)
                                  (projected-international-in-migrants params)
                                  (projected-international-out-migrants params)
                                  (:population-at-risk (loop/select-starting-popn prepared-inputs)))
                         (combine-into-net-flows)
                         :net-migration
                         (wds/join net-mig-r [:gss-code :sex :age]))]
      (is (every? #(fp-equals? (wds/subset-ds joined-mig :rows % :cols :net-mig-r)
                               (wds/subset-ds joined-mig :rows % :cols :net-mig)
                               0.000001)
                  (range (first (:shape joined-mig))))))))
