(ns witan.models.dem.ccm.mig.migration
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn merge->]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [witan.workspace-api.utils :as utils]
            [witan.models.dem.ccm.models-utils :as m-utils]
            [clojure.core.matrix.dataset :as ds]
            [schema.core :as s]))

(defworkflowfn projected-domestic-in-migrants
  {:witan/name :ccm-mig/proj-dom-in-mig
   :witan/version "1.0.0"
   :witan/input-schema {:domestic-in-migrants DomesticInmigrants}
   :witan/param-schema {:start-year-avg-domin-mig (s/constrained s/Int m-utils/year?)
                        :end-year-avg-domin-mig (s/constrained s/Int m-utils/year?)}
   :witan/output-schema {:projected-domestic-in-migrants ProjDomInSchema}}
  [{:keys [domestic-in-migrants]} {:keys [start-year-avg-domin-mig end-year-avg-domin-mig]}]
  {:projected-domestic-in-migrants
   (cf/first-projection-year-method-average domestic-in-migrants :domin
                                            :domestic-in start-year-avg-domin-mig end-year-avg-domin-mig)})

(defworkflowfn projected-domestic-out-migrants
  {:witan/name :ccm-mig/proj-dom-out-mig
   :witan/version "1.0.0"
   :witan/input-schema {:domestic-out-migrants DomesticOutmigrants}
   :witan/param-schema {:start-year-avg-domout-mig (s/constrained s/Int m-utils/year?)
                        :end-year-avg-domout-mig (s/constrained s/Int m-utils/year?)}
   :witan/output-schema {:projected-domestic-out-migrants ProjDomOutSchema}}
  [{:keys [domestic-out-migrants]} {:keys [start-year-avg-domout-mig end-year-avg-domout-mig]}]
  {:projected-domestic-out-migrants
   (cf/first-projection-year-method-average domestic-out-migrants :domout
                                            :domestic-out start-year-avg-domout-mig end-year-avg-domout-mig)})

(defn international-in-migrant-modifier
  "Takes in a dataset with projected in-migrants data, a column name to be modified and a multiplier"
  [projected-in-migrants-data col multiplier]
  (wds/add-derived-column projected-in-migrants-data col [col]
                          (fn [x] (* multiplier x))))

(defworkflowfn projected-international-in-migrants
  {:witan/name :ccm-mig/proj-inter-in-mig
   :witan/version "1.0.0"
   :witan/input-schema {:international-in-migrants InternationalInmigrants}
   :witan/param-schema {:start-year-avg-intin-mig (s/constrained s/Int m-utils/year?)
                        :end-year-avg-intin-mig (s/constrained s/Int m-utils/year?)
                        :in-migrant-multiplier (s/constrained s/Num number?)}
   :witan/output-schema {:projected-international-in-migrants ProjInterInSchema}}
  [{:keys [international-in-migrants]} {:keys [start-year-avg-intin-mig end-year-avg-intin-mig in-migrant-multiplier]}]
  {:projected-international-in-migrants
   (-> (cf/first-projection-year-method-average international-in-migrants :intin
                                                :international-in start-year-avg-intin-mig end-year-avg-intin-mig)
       (international-in-migrant-modifier :international-in in-migrant-multiplier))})

(defworkflowfn projected-international-out-migrants
  {:witan/name :ccm-mig/proj-inter-out-mig
   :witan/version "1.0.0"
   :witan/input-schema {:international-out-migrants InternationalOutmigrants}
   :witan/param-schema {:start-year-avg-intout-mig (s/constrained s/Int m-utils/year?)
                        :end-year-avg-intout-mig (s/constrained s/Int m-utils/year?)}
   :witan/output-schema {:projected-international-out-migrants ProjInterOutSchema}}
  [{:keys [international-out-migrants]} {:keys [start-year-avg-intout-mig end-year-avg-intout-mig]}]
  {:projected-international-out-migrants
   (cf/first-projection-year-method-average international-out-migrants :intout
                                            :international-out start-year-avg-intout-mig end-year-avg-intout-mig)})

(defworkflowfn combine-into-net-flows
  {:witan/name :ccm-mig/combine-mig-flows
   :witan/version "1.0.0"
   :witan/input-schema {:projected-domestic-in-migrants ProjDomInSchema
                        :projected-domestic-out-migrants ProjDomOutSchema
                        :projected-international-in-migrants ProjInterInSchema
                        :projected-international-out-migrants ProjInterOutSchema
                        :population-at-risk PopulationAtRiskSchema}
   :witan/output-schema {:net-migration NetMigrationSchema}}
  [{:keys [projected-domestic-in-migrants projected-domestic-out-migrants
           projected-international-in-migrants projected-international-out-migrants
           population-at-risk]} _]
  (let [n-net-migrants (wds/row-count projected-domestic-in-migrants)
        current-year (first (ds/column population-at-risk :year))
        net-migrants (-> (reduce #(wds/join %1 %2 [:gss-code :sex :age])
                                 [projected-domestic-in-migrants
                                  projected-domestic-out-migrants
                                  projected-international-in-migrants
                                  projected-international-out-migrants])
                         (wds/add-derived-column
                          :net-mig
                          [:domestic-in :domestic-out :international-in :international-out]
                          (fn [di do ii io] (+ (- di do) (- ii io))))
                         (ds/add-column :year (repeat n-net-migrants current-year))
                         (ds/select-columns [:gss-code :sex :age :year :net-mig]))]
    {:net-migration net-migrants}))
