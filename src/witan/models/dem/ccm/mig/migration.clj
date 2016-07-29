(ns witan.models.dem.ccm.mig.migration
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn merge->]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [schema.core :as s]))

(defworkflowfn project-domestic-in-migrants
  {:witan/name :ccm-mig/proj-dom-in-mig
   :witan/version "1.0"
   :witan/input-schema {:domestic-in-migrants DomesticInmigrants}
   :witan/param-schema {:start-yr-avg-domin-mig s/Int :end-yr-avg-domin-mig s/Int}
   :witan/output-schema {:projected-domestic-in-migrants ProjDomInSchema}
   :witan/exported? true}
  [{:keys [domestic-in-migrants]} {:keys [start-yr-avg-domin-mig end-yr-avg-domin-mig]}]
  {:projected-domestic-in-migrants
   (cf/jumpoffyr-method-average domestic-in-migrants :domin
                                :domestic-in start-yr-avg-domin-mig end-yr-avg-domin-mig)})

(defworkflowfn project-domestic-out-migrants
  {:witan/name :ccm-mig/proj-dom-out-mig
   :witan/version "1.0"
   :witan/input-schema {:domestic-out-migrants DomesticOutmigrants}
   :witan/param-schema {:start-yr-avg-domout-mig s/Int :end-yr-avg-domout-mig s/Int}
   :witan/output-schema {:projected-domestic-out-migrants ProjDomOutSchema}
   :witan/exported? true}
  [{:keys [domestic-out-migrants]} {:keys [start-yr-avg-domout-mig end-yr-avg-domout-mig]}]
  {:projected-domestic-out-migrants
   (cf/jumpoffyr-method-average domestic-out-migrants :domout
                                :domestic-out start-yr-avg-domout-mig end-yr-avg-domout-mig)})

(defworkflowfn project-international-in-migrants
  {:witan/name :ccm-mig/proj-inter-in-mig
   :witan/version "1.0"
   :witan/input-schema {:international-in-migrants InternationalInmigrants}
   :witan/param-schema {:start-yr-avg-intin-mig s/Int :end-yr-avg-intin-mig s/Int}
   :witan/output-schema {:projected-international-in-migrants ProjInterInSchema}
   :witan/exported? true}
  [{:keys [international-in-migrants]} {:keys [start-yr-avg-intin-mig end-yr-avg-intin-mig]}]
  {:projected-international-in-migrants
   (cf/jumpoffyr-method-average international-in-migrants :intin
                                :international-in start-yr-avg-intin-mig end-yr-avg-intin-mig)})

(defworkflowfn project-international-out-migrants
  {:witan/name :ccm-mig/proj-inter-out-mig
   :witan/version "1.0"
   :witan/input-schema {:international-out-migrants InternationalOutmigrants}
   :witan/param-schema {:start-yr-avg-intout-mig s/Int :end-yr-avg-intout-mig s/Int}
   :witan/output-schema {:projected-international-out-migrants ProjInterOutSchema}
   :witan/exported? true}
  [{:keys [international-out-migrants]} {:keys [start-yr-avg-intout-mig end-yr-avg-intout-mig]}]
  {:projected-international-out-migrants
   (cf/jumpoffyr-method-average international-out-migrants :intout
                                :international-out start-yr-avg-intout-mig end-yr-avg-intout-mig)})

(defworkflowfn combine-into-net-flows
  {:witan/name :ccm-mig/combine-mig-flows
   :witan/version "1.0"
   :witan/input-schema {:projected-domestic-in-migrants ProjDomInSchema
                        :projected-domestic-out-migrants ProjDomOutSchema
                        :projected-international-in-migrants ProjInterInSchema
                        :projected-international-out-migrants ProjInterOutSchema}
   :witan/output-schema {:net-migration NetMigrationSchema}
   :witan/exported? true}
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
