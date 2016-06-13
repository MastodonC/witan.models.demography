(ns witan.models.dem.ccm.mort.mortality-mvp
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [schema.core :as s]
            [incanter.core :as i]))

(defn create-popn-at-risk
  [popn-ds deaths-ds births-ds]
  (let [max-yr-deaths (reduce max (ds/column deaths-ds :year))
        popn-not-age-0 (-> popn-ds
                           (ds/emap-column :year inc)
                           (ds/emap-column :age (fn [v] (if (< v 90) (inc v) v)))
                           (wds/rollup :sum :popn [:gss-code :district :sex :age :year])
                           (i/query-dataset {:year {:$lte max-yr-deaths}}))]
    (-> births-ds
        (ds/rename-columns {:births :popn})
        (ds/join-rows popn-not-age-0))))

(defn calc-death-rates
  [deaths-ds popn-at-risk]
  (-> deaths-ds
      (wds/join popn-at-risk [:gss-code :district :sex :age :year])
      (wds/add-derived-column :death-rate [:deaths :popn]
                              (fn [d p] (wds/safe-divide [d p])))
      (ds/select-columns [:gss-code :district :sex :age :year :death-rate])))

(defworkflowfn calc-historic-asmr
  {:witan/name :ccm-mort/calc-historic-asmr
   :witan/version "1.0"
   :witan/input-schema {:historic-deaths DeathsSchema
                        :historic-births BirthsBySexAgeYearSchema
                        :historic-population HistPopulationSchema}
   :witan/output-schema {:historic-asmr HistASMRSchema}}
  [{:keys [historic-deaths historic-births historic-population]} _]
  (->> (create-popn-at-risk historic-population historic-deaths historic-births)
       (calc-death-rates historic-deaths)
       (hash-map :historic-asmr)))

(defworkflowfn project-asmr
  {:witan/name :ccm-mort/project-asmr
   :witan/version "1.0"
   :witan/input-schema {:historic-asmr HistASMRSchema}
   :witan/param-schema {:number-of-years-mort s/Int :jumpoff-year-mort s/Int}
   :witan/output-schema {:initial-projected-mortality-rates ProjFixedASMRSchema}}
  [{:keys [historic-asmr]} {:keys [number-of-years-mort jumpoff-year-mort]}]
  {:initial-projected-mortality-rates (cf/jumpoffyr-method-average historic-asmr
                                                                   :death-rate
                                                                   :death-rate
                                                                   number-of-years-mort
                                                                   jumpoff-year-mort)})

(defworkflowfn project-deaths-from-fixed-rates
  {:witan/name :ccm-mort/project-deaths-fixed-rates
   :witan/version "1.0"
   :witan/input-schema {:initial-projected-mortality-rates ProjFixedASMRSchema
                        :population-at-risk PopulationSchema}
   :witan/output-schema {:deaths DeathsOutputSchema}}
  [{:keys [initial-projected-mortality-rates population-at-risk]} _]
  {:deaths
   (cf/project-component-fixed-rates population-at-risk
                                     initial-projected-mortality-rates
                                     :death-rate :deaths)})
