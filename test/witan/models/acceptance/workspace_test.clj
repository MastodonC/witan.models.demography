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
            [witan.models.dem.ccm.core.projection-loop]))

(defworkflowfn resource-csv-loader
  "Loads CSV files from resources"
  {:witan/name :workspace-test/resource-csv-loader
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema {:src s/Str
                        :key s/Keyword}}
  [_ {:keys [src key]}]
  (ld/load-dataset key src))

(def tasks
  {;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Functions
   :proj-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/project-asfr-finalyrhist-fixed
                                :params {:fert-last-yr 2014
                                         :start-yr-avg-fert 2014
                                         :end-yr-avg-fert 2014}}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-deaths-from-fixed-rates}
   :proj-domestic-in-migrants  {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-in-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   :calc-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/calc-historic-asmr}
   :proj-domestic-out-migrants {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-out-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility-mvp/births-projection
                                :params {:proportion-male-newborns (double (/ 105 205))}}
   :proj-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-asmr
                                :params {:start-yr-avg-mort 2010
                                         :end-yr-avg-mort 2014}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :calc-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/calculate-historic-asfr
                                :params {:fert-last-yr 2014}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.net-migration/project-international-in-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.net-migration/project-international-out-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Predicates
   :finish-looping?            {:var #'witan.models.dem.ccm.core.projection-loop/finished-looping?
                                :params {:last-proj-year 2015}
                                :pred? true}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Inputs
   :in-historic-popn                  {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/bristol_hist_popn_mye.csv"
                                                :key :historic-population}}
   :in-historic-total-births          {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/fert/bristol_hist_births_mye.csv"
                                                :key :historic-births}}
   :in-projd-births-by-age-of-mother  {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                                                :key :ons-proj-births-by-age-mother}}
   :in-historic-deaths-by-age-and-sex {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/mort/bristol_hist_deaths_mye.csv"
                                                :key :historic-deaths}}
   :in-historic-dom-in-migrants       {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                                :key :domestic-in-migrants}}
   :in-historic-dom-out-migrants      {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                                :key :domestic-out-migrants}}
   :in-historic-intl-in-migrants      {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                                :key :international-in-migrants}}
   :in-historic-intl-out-migrants     {:var #'witan.models.acceptance.workspace-test/resource-csv-loader
                                       :params {:src "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                                                :key :international-out-migrants}}
   })

(def ccm-workflow
  [;; inputs for asfr
   [:in-historic-popn                 :calc-historic-asfr]
   [:in-historic-total-births         :calc-historic-asfr]
   [:in-projd-births-by-age-of-mother :calc-historic-asfr]

   ;; inputs for asmr
   [:in-historic-popn                  :calc-historic-asmr]
   [:in-historic-total-births          :calc-historic-asmr]
   [:in-historic-deaths-by-age-and-sex :calc-historic-asmr]

   ;; asfr/asmr projections
   [:calc-historic-asfr :proj-historic-asfr]
   [:calc-historic-asmr :proj-historic-asmr]

   ;; inputs for mig
   [:in-historic-dom-in-migrants   :proj-domestic-in-migrants]
   [:in-historic-dom-out-migrants  :proj-domestic-out-migrants]
   [:in-historic-intl-in-migrants  :proj-intl-in-migrants]
   [:in-historic-intl-out-migrants :proj-intl-out-migrants]

   ;; pre-loop merge
   [:proj-historic-asfr         :select-starting-popn]
   [:proj-historic-asmr         :select-starting-popn]
   [:proj-domestic-in-migrants  :select-starting-popn]
   [:proj-domestic-out-migrants :select-starting-popn]
   [:proj-intl-in-migrants      :select-starting-popn]
   [:proj-intl-out-migrants     :select-starting-popn]
   ;; - inputs for popn
   [:in-historic-popn           :select-starting-popn]

   ;; --- start popn loop
   [:select-starting-popn :project-births]
   [:select-starting-popn :project-deaths]
   [:project-births       :age-on]
   [:age-on               :add-births]
   [:add-births           :remove-deaths]
   [:project-deaths       :remove-deaths]
   [:remove-deaths        :apply-migration]
   [:apply-migration      :join-popn-latest-yr]
   [:join-popn-latest-yr  [:finish-looping? :out :select-starting-popn]]
   ;; --- end loop
   ])

(defn make-contracts
  [task-coll]
  (mapv (fn [[k v]]
          (let [meta-key (if (:pred? v) :witan/workflowpred :witan/workflowfn)]
            (-> v :var meta meta-key))) task-coll))

(defn make-catalog
  [tasks]
  (->> tasks
       (map :var)
       (map meta)))

(deftest workspace-test
  (let [workspace {:workflow ccm-workflow
                   :catalog (make-catalog tasks)
                   :contracts (make-contracts tasks)}]))
