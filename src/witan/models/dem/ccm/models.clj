(ns witan.models.dem.ccm.models
  (:require [witan.workspace-api :refer [defworkflowmodel]]))

(defworkflowmodel ccm-workflow
  "The CCM model"
  {:witan/name :test.model/ccm-model
   :witan/version "1.0"}
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
   ])
