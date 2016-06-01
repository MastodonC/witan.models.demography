(ns witan.models.dem.ccm.mort.calc-hist-asmr
  (:require [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [schema.core :as s]
            [witan.models.dem.ccm.schemas :refer :all]
            [witan.workspace-api :refer [defworkflowfn]]
            [witan.datasets :as wds]))

(defn create-popn-at-risk
  [popn-ds deaths-ds births-ds]
  (let [max-yr-deaths (reduce max (ds/column deaths-ds :year))
        inc-yr-aged-on (-> popn-ds
                           (ds/emap-column :year inc)
                           (ds/emap-column :age (fn [v] (if (< v 90) (inc v) v))))
        grouped (wds/rollup :sum :popn [:gss-code :district :sex :age :year] inc-yr-aged-on)
        filtered (i/query-dataset grouped {:year {:$lte max-yr-deaths}})]
    (-> births-ds
        (ds/rename-columns {:births :popn})
        (ds/join-rows filtered))))

(defn calc-death-rates
  [deaths-ds popn-at-risk]
  (let [popn-with-deaths (i/$join [[:gss-code :district :sex :age :year]
                                   [:gss-code :district :sex :age :year]]
                                  deaths-ds popn-at-risk)
        death-rates  (i/add-derived-column :death-rate
                                           [:deaths :popn]
                                           (fn [d p] (if (== p 0.0)
                                                       0.0
                                                       (/ d p))) popn-with-deaths)]
    (ds/select-columns death-rates [:gss-code :district :sex :age :year :death-rate])))

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
