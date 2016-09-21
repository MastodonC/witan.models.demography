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
    [:in-hist-popn                    :calc-hist-asfr]
    [:in-hist-total-births            :calc-hist-asfr]
    [:in-proj-births-by-age-of-mother :calc-hist-asfr]

    ;; inputs for asmr
    [:in-hist-popn                  :calc-hist-asmr]
    [:in-hist-total-births          :calc-hist-asmr]
    [:in-hist-deaths-by-age-and-sex :calc-hist-asmr]

    ;; asfr/asmr projections
    [:calc-hist-asfr       :project-asfr]
    [:in-future-fert-trend :project-asfr]
    [:calc-hist-asmr       :project-asmr]
    [:in-future-mort-trend :project-asmr]

    ;; inputs for mig
    [:in-hist-dom-in-migrants   :proj-dom-in-migrants]
    [:in-hist-dom-out-migrants  :proj-dom-out-migrants]
    [:in-hist-intl-in-migrants  :proj-intl-in-migrants]
    [:in-hist-intl-out-migrants :proj-intl-out-migrants]
    [:select-starting-popn   :combine-into-net-flows]

    ;; mig projections
    [:proj-dom-in-migrants   :combine-into-net-flows]
    [:proj-dom-out-migrants  :combine-into-net-flows]
    [:proj-intl-in-migrants  :combine-into-net-flows]
    [:proj-intl-out-migrants :combine-into-net-flows]
    [:select-starting-popn   :combine-into-net-flows]

    ;; inputs for loop
    [:in-hist-popn           :prepare-inputs]

    ;; pre-loop merge
    [:prepare-inputs         :select-starting-popn]
    [:project-asmr           :project-deaths]
    [:project-asfr           :project-births]
    [:in-hist-total-births   :append-by-year] ;; TODO historic births should come from `prepare-starting-popn'

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

    [:append-by-year [:finish-looping? :out :select-starting-popn]]
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
    {:witan/name :calc-hist-asfr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/calc-hist-asfr
     :witan/params {:fert-base-year 2014}}
    {:witan/name :calc-hist-asmr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mort/calc-historic-asmr}
    {:witan/name :finish-looping?
     :witan/version "1.0.0"
     :witan/type :predicate
     :witan/fn :ccm-core/ccm-loop-pred
     :witan/params {:last-proj-year 2018}}
    {:witan/name :in-hist-deaths-by-age-and-sex
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-deaths-by-age-and-sex
     :witan/params {:src (with-gss "witan.models.demography/mortality/historic_deaths")}}
    {:witan/name :in-hist-dom-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-dom-in-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_domestic_in")}}
    {:witan/name :in-hist-dom-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-dom-out-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_domestic_out")}}
    {:witan/name :in-hist-intl-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-intl-in-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_international_in")}}
    {:witan/name :in-hist-intl-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-intl-out-migrants
     :witan/params
     {:src (with-gss "witan.models.demography/migration/historic_migration_flows_international_out")}}
    {:witan/name :in-hist-popn
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-popn
     :witan/params
     {:src (with-gss "witan.models.demography/core/historic_population")}}
    {:witan/name :in-future-mort-trend
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-future-mort-trend
     :witan/params
     {:src "witan.models.demography/mortality/future_mortality_trend_assumption.csv.gz"}}
    {:witan/name :in-future-fert-trend
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-future-fert-trend
     :witan/params
     {:src "witan.models.demography/fertility/future_fertility_trend_assumption.csv.gz"}}
    {:witan/name :in-hist-total-births
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-hist-total-births
     :witan/params
     {:src (with-gss "witan.models.demography/fertility/historic_births")}}
    {:witan/name :in-proj-births-by-age-of-mother
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :ccm-core-input/in-proj-births-by-age-of-mother
     :witan/params
     {:src (with-gss "witan.models.demography/fertility/historic_births_by_age_of_mother")}}
    {:witan/name :append-by-year
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/append-years}
    {:witan/name :out
     :witan/version "1.0.0"
     :witan/type :output
     :witan/fn :ccm-core-out/ccm-out}
    {:witan/name :prepare-inputs
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/prepare-inputs
     :witan/params {:first-proj-year 2015}}
    {:witan/name :proj-dom-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-in-mig
     :witan/params {:start-year-avg-domin-mig 2003, :end-year-avg-domin-mig 2014}}
    {:witan/name :proj-dom-out-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-out-mig
     :witan/params {:start-year-avg-domout-mig 2003, :end-year-avg-domout-mig 2014}}
    {:witan/name :project-asfr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/project-asfr
     :witan/params {:fert-variant :applynationaltrend, :first-proj-year 2015, :last-proj-year 2018,
                    :fert-scenario :principal-2012}}
    {:witan/name :project-asmr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mort/project-asmr
     :witan/params {:start-year-avg-mort 2010, :end-year-avg-mort 2014,
                    :mort-variant :average-applynationaltrend, :first-proj-year 2015,
                    :last-proj-year 2018, :mort-scenario :principal}}
    {:witan/name :proj-intl-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-inter-in-mig
     :witan/params {:start-year-avg-intin-mig 2003,
                    :end-year-avg-intin-mig 2014}}
    {:witan/name :proj-intl-out-migrants
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
       inputs/in-hist-deaths-by-age-and-sex-1-0-0
       inputs/in-hist-dom-in-migrants-1-0-0
       inputs/in-hist-dom-out-migrants-1-0-0
       inputs/in-hist-intl-in-migrants-1-0-0
       inputs/in-hist-intl-out-migrants-1-0-0
       inputs/in-hist-popn-1-0-0
       inputs/in-future-mort-trend-1-0-0
       inputs/in-future-fert-trend-1-0-0
       inputs/in-hist-total-births-1-0-0
       inputs/in-proj-births-by-age-of-mother-1-0-0

       ;; fertility fns
       fert/project-asfr-1-0-0
       fert/project-births-1-0-0
       fert/combine-into-births-by-sex
       fert/calculate-historic-asfr

       ;; mortality fns
       mort/project-deaths-1-0-0
       mort/calc-historic-asmr
       mort/project-asmr-1-0-0

       ;; migration fns
       mig/project-domestic-in-migrants
       mig/project-domestic-out-migrants
       mig/project-international-in-migrants
       mig/project-international-out-migrants
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
