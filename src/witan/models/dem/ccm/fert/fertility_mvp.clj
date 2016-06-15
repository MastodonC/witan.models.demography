(ns witan.models.dem.ccm.fert.fertility-mvp
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [schema.core :as s]))

(defworkflowfn project-asfr-finalyrhist-fixed
  "Takes dataset of historic age specific fertility rates, and parameter
   for the last year of fertility data. Returns dataset with projected
   age specific fertility rates, calculated using the jumpoff year average
   method (see docs)."
  {:witan/name :ccm-fert/project-asfr-finalyrhist-fixed
   :witan/version "1.0"
   :witan/input-schema {:historic-asfr HistASFRSchema}
   :witan/param-schema {:fert-last-yr s/Int}
   :witan/output-schema {:initial-projected-fertility-rates ProjFixedASFRSchema}}
  [{:keys [historic-asfr]} {:keys [fert-last-yr]}]
  {:initial-projected-fertility-rates (cf/jumpoffyr-method-average historic-asfr
                                                                   :fert-rate
                                                                   :fert-rate
                                                                   1
                                                                   (inc fert-last-yr))})

(defn- gather-births-by-sex
  "Given a dataset with columns :gss-code, :m, and :f (where :m and :f are
   male and female births), gathers births data into :births column and 
   sex into :sex column, returning a new dataset. Standin for a universal 
   gather function in witan.datasets similar to gather in tidyR"
  [births-by-mf]
  (let [births-f (-> births-by-mf
                     (ds/add-column :sex (repeat "F"))
                     (ds/rename-columns {:f :births})
                     (ds/select-columns [:gss-code :sex :births]))
        births-m (-> births-by-mf
                     (ds/add-column :sex (repeat "M"))
                     (ds/rename-columns {:m :births})
                     (ds/select-columns [:gss-code :sex :births]))]
    (ds/join-rows births-m births-f)))

(defworkflowfn combine-into-births-by-sex
  "Takes dataset of historic age specific fertility rates, and parameter
   for the last year of fertility data. Returns dataset with projected
   age specific fertility rates, calculated using the jumpoff year average
   method (see docs)."
  {:witan/name :ccm-fert/combine-into-births-by-sex
   :witan/version "1.0"
   :witan/input-schema {:births-by-age-sex-mother BirthsAgeSexMotherSchema}
   :witan/param-schema {:pm double}
   :witan/output-schema {:births-by-sex BirthsBySexSchema}}
  [{:keys [births-by-age-sex-mother]} {:keys [pm]}]
  (let [births-by-sex (-> births-by-age-sex-mother
                          (wds/rollup :sum :births [:gss-code])
                          (wds/add-derived-column :m [:births]
                                                  (fn [b] (double (* pm b))))
                          (wds/add-derived-column :f [:births]
                                                  (fn [b] (double (* (- 1 pm) b))))
                          (ds/select-columns [:gss-code :m :f])
                          gather-births-by-sex)]
    {:births-by-sex births-by-sex}))
