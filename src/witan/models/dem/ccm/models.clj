(ns witan.models.dem.ccm.models
  (:require [witan.workspace-api :refer [defmodel
                                         definput]]
            [witan.workspace-api.utils :refer [map-fn-meta
                                               map-model-meta]]
            [witan.workspace-api.protocols :as p]
            [witan.models.dem.ccm.inputs           :as inputs]
            [witan.models.dem.ccm.mort.mortality   :as mort]
            [witan.models.dem.ccm.fert.fertility   :as fert]
            [witan.models.dem.ccm.mig.migration    :as mig]
            [witan.models.dem.ccm.core.projection-loop :as core]))

(defn with-gss
  [id]
  (str id "_{{GSS-Code}}.csv.gz"))

(defmodel cohort-component-model
  "The CCM model"
  {:witan/name :ccm-model/cohort-component-model
   :witan/version "1.0.0"}
  {:workflow
   [;; inputs for asfr
    [:historic-population           :calculate-historic-asfr]
    [:historic-births               :calculate-historic-asfr]
    [:historic-births-by-age-mother :calculate-historic-asfr]

    ;; inputs for asmr
    [:historic-population  :calculate-historic-asmr]
    [:historic-births      :calculate-historic-asmr]
    [:historic-deaths      :calculate-historic-asmr]

    ;; asfr/asmr projections
    [:calculate-historic-asfr            :project-asfr]
    [:future-fertility-trend-assumption  :project-asfr]
    [:calculate-historic-asmr            :project-asmr]
    [:future-mortality-trend-assumption  :project-asmr]

    ;; inputs for mig
    [:domestic-in-migrants       :projected-domestic-in-migrants]
    [:domestic-out-migrants      :projected-domestic-out-migrants]
    [:international-in-migrants  :projected-international-in-migrants]
    [:international-out-migrants :projected-international-out-migrants]

    ;; mig projections
    [:projected-domestic-in-migrants       :combine-into-net-flows]
    [:projected-domestic-out-migrants      :combine-into-net-flows]
    [:projected-international-in-migrants  :combine-into-net-flows]
    [:projected-international-out-migrants :combine-into-net-flows]

    ;; inputs for loop
    [:historic-population    :prepare-inputs]

    ;; pre-loop merge
    [:prepare-inputs         :select-starting-popn]
    [:project-asmr           :project-deaths]
    [:project-asfr           :project-births]
    [:historic-births        :append-by-year]

    ;; --- start popn loop
    [:select-starting-popn       :project-births]
    [:select-starting-popn       :project-deaths]
    [:select-starting-popn       :age-on]
    [:select-starting-popn       :append-by-year]
    [:select-starting-popn       :combine-into-net-flows]

    [:project-births             :combine-into-births-by-sex]
    [:combine-into-births-by-sex :add-births]
    [:age-on                     :add-births]
    [:add-births                 :remove-deaths]
    [:project-deaths             :remove-deaths]
    [:remove-deaths              :apply-migration]
    [:combine-into-net-flows     :apply-migration]
    [:apply-migration            :finalise-popn]
    [:project-deaths             :append-by-year]
    [:finalise-popn              :append-by-year]
    [:combine-into-births-by-sex :append-by-year]
    [:combine-into-net-flows     :append-by-year]

    [:append-by-year [:finish-looping? :ccm-out :select-starting-popn]]
    ;; --- end loop
    ]
   :catalog
   [{:witan/name :add-births
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/add-births}
    {:witan/name :age-on
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/age-on}
    {:witan/name :apply-migration
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/apply-migration}
    {:witan/name :calculate-historic-asfr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/calculate-historic-asfr
     :witan/params {:fert-base-year 2014}}
    {:witan/name :calculate-historic-asmr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mort/calculate-historic-asmr}
    {:witan/name :finish-looping?
     :witan/version "1.0.0"
     :witan/type :predicate
     :witan/fn :ccm-core/ccm-loop-pred
     :witan/params {:last-proj-year 2018}}
    {:witan/name :historic-deaths
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/historic-deaths
     :witan/params {:src (with-gss "witan.models.demography/mortality/historic_deaths")}}
    {:witan/name :domestic-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/domestic-in-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_domestic_in")}}
    {:witan/name :domestic-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/domestic-out-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_domestic_out")}}
    {:witan/name :international-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/international-in-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_international_in")}}
    {:witan/name :international-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/international-out-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_international_out")}}
    {:witan/name :historic-population
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/historic-population
     :witan/params
     {:src (with-gss "witan.models.demography/core/historic_population")}}
    {:witan/name :future-mortality-trend-assumption
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/future-mortality-trend-assumption
     :witan/params
     {:src "witan.models.demography/mortality/future_mortality_trend_assumption.csv.gz"}}
    {:witan/name :future-fertility-trend-assumption
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/future-fertility-trend-assumption
     :witan/params
     {:src "witan.models.demography/fertility/future_fertility_trend_assumption.csv.gz"}}
    {:witan/name :historic-births
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/historic-births
     :witan/params
     {:src (with-gss "witan.models.demography/fertility/historic_births")}}
    {:witan/name :historic-births-by-age-mother
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/historic-births-by-age-mother
     :witan/params
     {:src (with-gss "witan.models.demography/fertility/historic_births_by_age_of_mother")}}
    {:witan/name :append-by-year
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/append-years}
    {:witan/name :ccm-out
     :witan/version "1.0.0"
     :witan/type :output
     :witan/fn :ccm-core-out/ccm-out}
    {:witan/name :prepare-inputs
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/prepare-inputs
     :witan/params {:first-proj-year 2015}}
    {:witan/name :projected-domestic-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-in-mig
     :witan/params {:start-year-avg-domin-mig 2003, :end-year-avg-domin-mig 2014}}
    {:witan/name :projected-domestic-out-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-out-mig
     :witan/params {:start-year-avg-domout-mig 2003, :end-year-avg-domout-mig 2014}}
    {:witan/name :project-asfr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/project-asfr
     :witan/params {:proj-asfr-variant :applynationaltrend, :first-proj-year 2015, :last-proj-year 2018,
                    :future-fert-scenario :principal-2012}}
    {:witan/name :project-asmr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mort/project-asmr
     :witan/params {:start-year-avg-mort 2010, :end-year-avg-mort 2014,
                    :proj-asmr-variant :average-applynationaltrend, :first-proj-year 2015,
                    :last-proj-year 2018, :mort-scenario :principal}}
    {:witan/name :projected-international-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-inter-in-mig
     :witan/params {:start-year-avg-intin-mig 2003,:end-year-avg-intin-mig 2014}}
    {:witan/name :projected-international-out-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-inter-out-mig
     :witan/params {:start-year-avg-intout-mig 2003, :end-year-avg-intout-mig 2014}}
    {:witan/name :combine-into-births-by-sex
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/combine-into-births-by-sex
     :witan/params {:proportion-male-newborns (double (/ 105 205))}}
    {:witan/name :combine-into-net-flows
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/combine-mig-flows}
    {:witan/name :project-births
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/project-births}
    {:witan/name :project-deaths
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mort/project-deaths}
    {:witan/name :remove-deaths
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/remove-deaths}
    {:witan/name :select-starting-popn
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/select-starting-popn}
    {:witan/name :finalise-popn
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/finalise-popn}]})

(defn model-library
  []
  (reify p/IModelLibrary
    (available-fns [_]
      (map-fn-meta
       ;; inputs
       inputs/historic-deaths-1-0-0
       inputs/domestic-in-migrants-1-0-0
       inputs/domestic-out-migrants-1-0-0
       inputs/international-in-migrants-1-0-0
       inputs/international-out-migrants-1-0-0
       inputs/historic-population-1-0-0
       inputs/future-mortality-trend-assumption-1-0-0
       inputs/future-fertility-trend-assumption-1-0-0
       inputs/historic-births-1-0-0
       inputs/historic-births-by-age-mother-1-0-0

       ;; fertility fns
       fert/project-asfr-1-0-0
       fert/project-births-1-0-0
       fert/combine-into-births-by-sex
       fert/calculate-historic-asfr

       ;; mortality fns
       mort/project-deaths-1-0-0
       mort/calculate-historic-asmr
       mort/project-asmr-1-0-0

       ;; migration fns
       mig/projected-domestic-in-migrants
       mig/projected-domestic-out-migrants
       mig/projected-international-in-migrants
       mig/projected-international-out-migrants
       mig/combine-into-net-flows

       ;; core fns
       core/append-by-year-1-0-0
       core/finalise-popn-1-0-0
       core/add-births
       core/remove-deaths
       core/age-on
       core/select-starting-popn
       core/prepare-inputs-1-0-0
       core/apply-migration

       ;; core preds
       core/finished-looping?

       ;; core outputs
       core/ccm-out))
    (available-models [_]
      (map-model-meta cohort-component-model))))
