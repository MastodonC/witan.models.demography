(ns witan.models.dem.ccm.models
  (:require [witan.workspace-api :refer [defmodel]]
            [witan.workspace-api.utils :refer [map-fn-meta
                                               map-model-meta]]
            [witan.workspace-api.protocols :as p]
            [witan.models.dem.ccm.mort.mortality   :as mort]
            [witan.models.dem.ccm.fert.fertility   :as fert]
            [witan.models.dem.ccm.mig.migration    :as mig]
            [witan.models.dem.ccm.core.projection-loop :as core]))

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
    [:calc-hist-asfr :project-asfr]
    [:calc-hist-asmr :project-asmr]
    [:in-future-mort-trend :project-asmr]

    ;; inputs for mig
    [:in-hist-dom-in-migrants   :proj-dom-in-migrants]
    [:in-hist-dom-out-migrants  :proj-dom-out-migrants]
    [:in-hist-intl-in-migrants  :proj-intl-in-migrants]
    [:in-hist-intl-out-migrants :proj-intl-out-migrants]

    ;; mig projections
    [:proj-dom-in-migrants   :combine-into-net-flows]
    [:proj-dom-out-migrants  :combine-into-net-flows]
    [:proj-intl-in-migrants  :combine-into-net-flows]
    [:proj-intl-out-migrants :combine-into-net-flows]

    ;; inputs for loop
    [:in-hist-popn           :prepare-starting-popn]

    ;; pre-loop merge
    [:prepare-starting-popn  :select-starting-popn]
    [:project-asfr           :select-starting-popn]
    [:project-asmr           :select-starting-popn]
    [:combine-into-net-flows :select-starting-popn]

    ;; --- start popn loop
    [:select-starting-popn       :project-births]
    [:select-starting-popn       :project-deaths]
    [:project-births             :combine-into-births-by-sex]
    [:combine-into-births-by-sex :age-on]
    [:age-on                     :add-births]
    [:add-births                 :remove-deaths]
    [:project-deaths             :remove-deaths]
    [:remove-deaths              :apply-migration]
    [:apply-migration            :join-popn-latest-year]
    [:join-popn-latest-year      [:finish-looping? :out :select-starting-popn]]
    ;; --- end loop
    ]
   :catalog
   [{:witan/name :add-births
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/add-births}
    {:witan/name :age-on, :witan/version "1.0.0", :witan/type :function, :witan/fn :ccm-cor/age-on}
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
     :witan/params {:last-proj-year 2021}}
    {:witan/name :in-hist-deaths-by-age-and-sex
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/mortality/historic_deaths.csv"
      :key :historic-deaths}}
    {:witan/name :in-hist-dom-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/migration/domestic_in_migrants.csv"
      :key :domestic-in-migrants}}
    {:witan/name :in-hist-dom-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/migration/domestic_out_migrants.csv"
      :key :domestic-out-migrants}}
    {:witan/name :in-hist-intl-in-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/migration/international_in_migrants.csv"
      :key :international-in-migrants}}
    {:witan/name :in-hist-intl-out-migrants
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/migration/international_out_migrants.csv"
      :key :international-out-migrants}}
    {:witan/name :in-hist-popn
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/core/historic-population.csv"
      :key :historic-population}}
    {:witan/name :in-future-mort-trend
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/mortality/death_improvement.csv"
      :key :future-mortality-trend-assumption}}
    {:witan/name :in-hist-total-births
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src "witan.models.demography/fertility/historic_births.csv"
      :key :historic-births}}
    {:witan/name :in-proj-births-by-age-of-mother
     :witan/version "1.0.0"
     :witan/type :input
     :witan/fn :workspace-test/resource-csv-loader
     :witan/params
     {:src "witan.models.demography/fertility/proj-births-by-age-mother.csv"
      :key :historic-births-by-age-mother}}
    {:witan/name :join-popn-latest-year
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/join-years}
    {:witan/name :out
     :witan/version "1.0.0"
     :witan/type :output
     :witan/fn :ccm-core-out/population}
    {:witan/name :prepare-starting-popn
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-core/prepare-starting-popn}
    {:witan/name :proj-dom-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-in-mig
     :witan/params {:start-year-avg-dom-mig 2003, :end-year-avg-dom-mig 2014}}
    {:witan/name :proj-dom-out-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-dom-out-mig
     :witan/params {:start-year-avg-dom-mig 2003, :end-year-avg-dom-mig 2014}}
    {:witan/name :project-asfr
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/project-asfr-finalyearhist-fixed
     :witan/params {:fert-base-year 2014, :start-year-avg-fert 2014, :end-year-avg-fert 2014}}
    {:witan/name :project-asmr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mort/project-asmr
     :witan/params {:start-year-avg-mort 2010, :end-year-avg-mort 2014, :last-proj-year 2021 :first-proj-year 2014}}
    {:witan/name :proj-intl-in-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-inter-in-mig
     :witan/params {:start-year-avg-inter-mig 2003, :end-year-avg-inter-mig 2014}}
    {:witan/name :proj-intl-out-migrants
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-mig/proj-inter-out-mig
     :witan/params {:start-year-avg-inter-mig 2003, :end-year-avg-inter-mig 2014}}
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
     :witan/fn :ccm-fert/project-births-fixed-rates}
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
     :witan/fn :ccm-core/select-starting-popn}]})

(defn model-library
  []
  (reify p/IModelLibrary
    (available-fns [_]
      (map-fn-meta
       ;; fertility fns
       fert/project-asfr-finalyearhist-fixed
       fert/project-births-from-fixed-rates
       fert/combine-into-births-by-sex
       fert/calculate-historic-asfr

       ;; mortality fns
       mort/project-deaths
       mort/calc-historic-asmr
       mort/project-asmr-1-0-0

       ;; migration fns
       mig/project-domestic-in-migrants
       mig/project-domestic-out-migrants
       mig/project-international-in-migrants
       mig/project-international-out-migrants
       mig/combine-into-net-flows

       ;; core fns
       core/join-popn-latest-year
       core/add-births
       core/remove-deaths
       core/age-on
       core/select-starting-popn
       core/prepare-inputs
       core/apply-migration

       ;; core preds
       core/finished-looping?

       ;; core outputs
       core/population-out))
    (available-models [_]
      (map-model-meta cohort-component-model))))
