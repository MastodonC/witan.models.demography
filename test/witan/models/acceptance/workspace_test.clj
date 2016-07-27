(ns witan.models.acceptance.workspace-test
  (:require [clojure.test :refer :all]
            [witan.workspace-api :refer :all]
            [witan.workspace-executor.core :as wex]
            [witan.models.load-data :as ld]
            [schema.core :as s]
            ;;
            [witan.models.dem.ccm.fert.fertility-mvp]
            [witan.models.dem.ccm.mort.mortality-mvp]
            [witan.models.dem.ccm.mig.net-migration]
            [witan.models.dem.ccm.core.projection-loop]
            [witan.models.dem.ccm.models :refer [ccm-workflow]]
            [witan.models.dem.ccm.models-utils :refer [make-catalog make-contracts]]))



(def tasks
  {;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Functions
   :project-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/project-asfr-finalyrhist-fixed
                                :params {:fert-last-yr 2014
                                         :start-yr-avg-fert 2014
                                         :end-yr-avg-fert 2014}}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-deaths-from-fixed-rates}
   :proj-dom-in-migrants  {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-in-migrants
                                :params {:start-yr-avg-domin-mig 2003
                                         :end-yr-avg-domin-mig 2014}}
   :calc-hist-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/calc-historic-asmr}
   :proj-dom-out-migrants {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-out-migrants
                                :params {:start-yr-avg-domout-mig 2003
                                         :end-yr-avg-domout-mig 2014}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility-mvp/project-births-from-fixed-rates}
   :combine-into-births-by-sex {:var #'witan.models.dem.ccm.fert.fertility-mvp/combine-into-births-by-sex
                                :params {:proportion-male-newborns (double (/ 105 205))}}
   :project-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-asmr
                                :params {:start-yr-avg-mort 2010
                                         :end-yr-avg-mort 2014}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :prepare-starting-popn      {:var #'witan.models.dem.ccm.core.projection-loop/prepare-inputs}
   :calc-hist-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/calculate-historic-asfr
                                :params {:fert-last-yr 2014}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.net-migration/project-international-in-migrants
                                :params {:start-yr-avg-intin-mig 2003
                                         :end-yr-avg-intin-mig 2014}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.net-migration/project-international-out-migrants
                                :params {:start-yr-avg-intout-mig 2003
                                         :end-yr-avg-intout-mig 2014}}

   :combine-into-net-flows {:var #'witan.models.dem.ccm.mig.net-migration/combine-into-net-flows}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Predicates
   :finish-looping?            {:var #'witan.models.dem.ccm.core.projection-loop/finished-looping?
                                :params {:last-proj-year 2021}
                                :pred? true}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Inputs
   :in-hist-popn                  {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"
                                                :key :historic-population}}
   :in-hist-total-births          {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"
                                                :key :historic-births}}
   :in-proj-births-by-age-of-mother  {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                                                :key :ons-proj-births-by-age-mother}}
   :in-hist-deaths-by-age-and-sex {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                                                :key :historic-deaths}}
   :in-hist-dom-in-migrants       {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                                :key :domestic-in-migrants}}
   :in-hist-dom-out-migrants      {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                                :key :domestic-out-migrants}}
   :in-hist-intl-in-migrants      {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                                :key :international-in-migrants}}
   :in-hist-intl-out-migrants     {:var #'witan.models.dem.ccm.models-utils/resource-csv-loader
                                       :params {:src "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                                                :key :international-out-migrants}}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Outputs
   :out {:var #'witan.models.dem.ccm.models-utils/out}
   })


(deftest workspace-test
  (let [workspace     {:workflow  ccm-workflow
                       :catalog   (make-catalog tasks)
                       :contracts (make-contracts tasks)}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})
        expected-keys (sort [:births
                             :international-in-migrants
                             :projected-international-in-migrants
                             :historic-asfr
                             :latest-yr-popn
                             :initial-projected-fertility-rates
                             :deaths
                             :historic-births
                             :historic-deaths
                             :births-by-age-sex-mother
                             :domestic-out-migrants
                             :historic-asmr
                             :projected-domestic-in-migrants
                             :ons-proj-births-by-age-mother
                             :net-migration
                             :finished?
                             :initial-projected-mortality-rates
                             :domestic-in-migrants
                             :loop-year
                             :projected-domestic-out-migrants
                             :projected-international-out-migrants
                             :population-at-risk
                             :historic-population
                             :international-out-migrants])]
    (is result)
    (is (= expected-keys (-> result first keys vec sort)))
    (is (:finished? (first result)))))
