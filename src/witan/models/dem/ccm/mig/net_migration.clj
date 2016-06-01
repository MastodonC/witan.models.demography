(ns witan.models.dem.ccm.mig.net-migration
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [schema.core :as s]))

(defworkflowfn project-domestic-in-migrants
  {:witan/name :ccm-mig/proj-dom-in-mig
   :witan/version "1.0"
   :witan/input-schema {:domestic-in-migrants ComponentMYESchema}
   :witan/param-schema {:number-of-years s/Int :jumpoff-year s/Int}
   :witan/output-schema {:projected-domestic-in-migrants ProjDomInSchema}}
  [{:keys [domestic-in-migrants]} {:keys [number-of-years jumpoff-year]}]
  {:projected-domestic-in-migrants
   (cf/jumpoffyr-method-average domestic-in-migrants :estimate
                          :domestic-in number-of-years jumpoff-year)})

(defworkflowfn project-domestic-out-migrants
  {:witan/name :ccm-mig/proj-dom-out-mig
   :witan/version "1.0"
   :witan/input-schema {:domestic-out-migrants ComponentMYESchema}
   :witan/param-schema {:number-of-years s/Int :jumpoff-year s/Int}
   :witan/output-schema {:projected-domestic-out-migrants ProjDomOutSchema}}
  [{:keys [domestic-out-migrants]} {:keys [number-of-years jumpoff-year]}]
  {:projected-domestic-out-migrants
   (cf/jumpoffyr-method-average domestic-out-migrants :estimate
                          :domestic-out number-of-years jumpoff-year)})

(defworkflowfn project-international-in-migrants
  {:witan/name :ccm-mig/proj-inter-in-mig
   :witan/version "1.0"
   :witan/input-schema {:international-in-migrants ComponentMYESchema}
   :witan/param-schema {:number-of-years s/Int :jumpoff-year s/Int}
   :witan/output-schema {:projected-international-in-migrants ProjInterInSchema}}
  [{:keys [international-in-migrants]} {:keys [number-of-years jumpoff-year]}]
  {:projected-international-in-migrants
   (cf/jumpoffyr-method-average international-in-migrants :estimate
                          :international-in number-of-years jumpoff-year)})

(defworkflowfn project-international-out-migrants
  {:witan/name :ccm-mig/proj-inter-out-mig
   :witan/version "1.0"
   :witan/input-schema {:international-out-migrants ComponentMYESchema}
   :witan/param-schema {:number-of-years s/Int :jumpoff-year s/Int}
   :witan/output-schema {:projected-international-out-migrants ProjInterOutSchema}}
  [{:keys [international-out-migrants]} {:keys [number-of-years jumpoff-year]}]
  {:projected-international-out-migrants
   (cf/jumpoffyr-method-average international-out-migrants :estimate
                          :international-out number-of-years jumpoff-year)})

(defworkflowfn combine-into-net-flows
  {:witan/name :ccm-mig/combine-mig-flows
   :witan/version "1.0"
   :witan/input-schema {:projected-domestic-in-migrants ProjDomInSchema
                        :projected-domestic-out-migrants ProjDomOutSchema
                        :projected-international-in-migrants ProjInterInSchema
                        :projected-international-out-migrants ProjInterOutSchema}
   :witan/output-schema {:net-migration NetMigrationSchema}}
  [{:keys [projected-domestic-in-migrants projected-domestic-out-migrants
           projected-international-in-migrants projected-international-out-migrants]} _]
  (let [net-migrants (-> (reduce #(wds/join %1 %2 [:gss-code :sex :age])
                                 [projected-domestic-in-migrants
                                  projected-domestic-out-migrants
                                  projected-international-in-migrants
                                  projected-international-out-migrants])
                         (wds/add-derived-column
                          :net-mig
                          [:domestic-in :domestic-out :international-in :international-out]
                          (fn [di do ii io] (+ (- di do) (- ii io))))
                         (ds/select-columns [:gss-code :sex :age :net-mig]))]
    {:net-migration net-migrants}))
