(ns witan.models.dem.ccm.models
  (:require [witan.workspace-api :refer [defmodel]]
            [witan.workspace-api.utils :refer [map-fn-meta
                                               map-model-meta]]
            [witan.workspace-api.protocols :as p]
            ;;
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
    [:apply-migration            :join-popn-latest-yr]
    [:join-popn-latest-yr        [:finish-looping? :out :select-starting-popn]]
    ;; --- end loop
    ]
   :catalog
   [{:witan/name :add-births,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/add-births}
    {:witan/name :age-on, :witan/version "1.0.0", :witan/type :function, :witan/fn :ccm-cor/age-on}
    {:witan/name :apply-migration,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/apply-migration}
    {:witan/name :calc-hist-asfr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-fert/calc-hist-asfr,
     :witan/params {:fert-last-yr 2014}}
    {:witan/name :calc-hist-asmr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mort/calc-historic-asmr}
    {:witan/name :finish-looping?,
     :witan/version "1.0.0",
     :witan/type :predicate,
     :witan/fn :ccm-core/ccm-loop-pred,
     :witan/params {:last-proj-year 2021}}
    {:witan/name :in-hist-deaths-by-age-and-sex,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :historic-deaths}}
    {:witan/name :in-hist-dom-in-migrants,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :domestic-in-migrants}}
    {:witan/name :in-hist-dom-out-migrants,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :domestic-out-migrants}}
    {:witan/name :in-hist-intl-in-migrants,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :international-in-migrants}}
    {:witan/name :in-hist-intl-out-migrants,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :international-out-migrants}}
    {:witan/name :in-hist-popn,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :historic-population}}
    {:witan/name :in-hist-total-births,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :historic-births}}
    {:witan/name :in-proj-births-by-age-of-mother,
     :witan/version "1.0.0",
     :witan/type :input,
     :witan/fn :workspace-test/resource-csv-loader,
     :witan/params
     {:src nil,
      :key :ons-proj-births-by-age-mother}}
    {:witan/name :join-popn-latest-yr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/join-yrs}
    {:witan/name :out, :witan/version "1.0.0", :witan/type :output, :witan/fn :workspace-test/out}
    {:witan/name :prepare-starting-popn,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/prepare-starting-popn}
    {:witan/name :proj-dom-in-migrants,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mig/proj-dom-in-mig,
     :witan/params {:start-yr-avg-dom-mig 2003, :end-yr-avg-dom-mig 2014}}
    {:witan/name :proj-dom-out-migrants,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mig/proj-dom-out-mig,
     :witan/params {:start-yr-avg-dom-mig 2003, :end-yr-avg-dom-mig 2014}}
    {:witan/name :project-asfr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-fert/project-asfr-finalyrhist-fixed,
     :witan/params {:fert-last-yr 2014, :start-yr-avg-fert 2014, :end-yr-avg-fert 2014}}
    {:witan/name :project-asmr,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mort/project-asmr-average-fixed,
     :witan/params {:start-yr-avg-mort 2010, :end-yr-avg-mort 2014}}
    {:witan/name :proj-intl-in-migrants,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mig/proj-inter-in-mig,
     :witan/params {:start-yr-avg-inter-mig 2003, :end-yr-avg-inter-mig 2014}}
    {:witan/name :proj-intl-out-migrants,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mig/proj-inter-out-mig,
     :witan/params {:start-yr-avg-inter-mig 2003, :end-yr-avg-inter-mig 2014}}
    {:witan/name :combine-into-births-by-sex
     :witan/version "1.0.0"
     :witan/type :function
     :witan/fn :ccm-fert/combine-into-births-by-sex
     :witan/params {:proportion-male-newborns (double (/ 105 205))}}
    {:witan/name :combine-into-net-flows
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mig/combine-mig-flows}
    {:witan/name :project-births,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-fert/births-projection,
     :witan/params {:proportion-male-newborns 0.5121951219512195}}
    {:witan/name :project-deaths,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-mort/project-deaths-fixed-rates}
    {:witan/name :remove-deaths,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/remove-deaths}
    {:witan/name :select-starting-popn,
     :witan/version "1.0.0",
     :witan/type :function,
     :witan/fn :ccm-core/select-starting-popn}]})

(defn model-library
  []
  (reify p/IModelLibrary
    (available-fns [_]
      (map-fn-meta
       fert/project-asfr-finalyrhist-fixed
       fert/project-births-from-fixed-rates
       fert/combine-into-births-by-sex
       fert/calculate-historic-asfr

       mort/project-deaths-from-fixed-rates
       mort/calc-historic-asmr
       mort/project-asmr-average-fixed

       mig/project-domestic-in-migrants
       mig/project-domestic-out-migrants
       mig/project-international-in-migrants
       mig/project-international-out-migrants
       mig/combine-into-net-flows

       core/join-popn-latest-yr
       core/add-births
       core/remove-deaths
       core/age-on
       core/select-starting-popn
       core/prepare-inputs
       core/apply-migration

       core/finished-looping?))
    (available-models [_]
      (map-model-meta cohort-component-model))))
