(ns witan.models.acceptance.workspace-test
  (:require [clojure.test :refer :all]
            [clojure.set]
            [witan.workspace-api :refer :all]
            [witan.workspace-api.protocols :as p]
            [witan.workspace-executor.core :as wex]
            [witan.models.load-data :as ld]
            [schema.core :as s]
            ;;
            [witan.models.dem.ccm.fert.fertility]
            [witan.models.dem.ccm.mort.mortality]
            [witan.models.dem.ccm.mig.migration]
            [witan.models.dem.ccm.core.projection-loop]
            [witan.models.dem.ccm.models :refer [cohort-component-model
                                                 model-library]]
            [witan.models.dem.ccm.models-utils :refer [make-catalog make-contracts]]))



(def tasks
  { ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Functions
   :project-asfr               {:var #'witan.models.dem.ccm.fert.fertility/project-asfr-finalyrhist-fixed}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality/project-deaths-from-fixed-rates}
   :proj-dom-in-migrants       {:var #'witan.models.dem.ccm.mig.migration/project-domestic-in-migrants
                                :params {:start-yr-avg-domin-mig 2003
                                         :end-yr-avg-domin-mig 2014}}
   :calc-hist-asmr             {:var #'witan.models.dem.ccm.mort.mortality/calc-historic-asmr}
   :proj-dom-out-migrants      {:var #'witan.models.dem.ccm.mig.migration/project-domestic-out-migrants
                                :params {:start-yr-avg-domout-mig 2003
                                         :end-yr-avg-domout-mig 2014}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility/project-births-from-fixed-rates}
   :combine-into-births-by-sex {:var #'witan.models.dem.ccm.fert.fertility/combine-into-births-by-sex
                                :params {:proportion-male-newborns (double (/ 105 205))}}
   :project-asmr               {:var #'witan.models.dem.ccm.mort.mortality/project-asmr
                                :params {:start-yr-avg-mort 2010
                                         :end-yr-avg-mort 2014}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :prepare-starting-popn      {:var #'witan.models.dem.ccm.core.projection-loop/prepare-inputs}
   :calc-hist-asfr             {:var #'witan.models.dem.ccm.fert.fertility/calculate-historic-asfr
                                :params {:fert-base-yr 2014}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.migration/project-international-in-migrants
                                :params {:start-yr-avg-intin-mig 2003
                                         :end-yr-avg-intin-mig 2014}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.migration/project-international-out-migrants
                                :params {:start-yr-avg-intout-mig 2003
                                         :end-yr-avg-intout-mig 2014}}

   :combine-into-net-flows {:var #'witan.models.dem.ccm.mig.migration/combine-into-net-flows}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Predicates
   :finish-looping?            {:var #'witan.models.dem.ccm.core.projection-loop/finished-looping?
                                :params {:last-proj-year 2021}}
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
  (let [workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   (make-catalog tasks)
                       :contracts (make-contracts tasks)}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})]
    (is (not-empty result))
    (is (:finished? (first result)))))

(deftest imodellibrary-test
  (let [ml (model-library)
        models (p/available-models ml)
        fns (p/available-fns ml)]
    (is (not-empty models))
    (is (= (first models) (-> #'cohort-component-model meta :witan/metadata)))
    (is (not-empty fns))))
