(ns witan.models.dem.ccm.mort.mortality
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [witan.workspace-api.utils :as utils]
            [witan.models.dem.ccm.models-utils :as m-utils]
            [clojure.core.matrix.dataset :as ds]
            [schema.core :as s]))

(defn create-popn-at-risk-death
  "Creates population at risk for calculating historic age specific mortality rates.
  Takes datasets with population, deaths, and births. Returns a new dataset with
  population at risk in the :popn column. The population at risk is the births data
  (age 0) joined with the aged on population dataset (age 1 to 90) for years less than
  or equal to the latest year of deaths data.."
  [popn-ds deaths-ds births-ds]
  (let [max-yr-deaths (reduce max (ds/column deaths-ds :year))
        _ (utils/property-holds? max-yr-deaths m-utils/year? (str max-yr-deaths " is not a year"))
        popn-not-age-0 (-> popn-ds
                           (ds/emap-column :year inc)
                           (wds/select-from-ds {:year {:$lte max-yr-deaths}})
                           (ds/emap-column :age (fn [v] (if (< v 90) (inc v) v)))
                           (wds/rollup :sum :popn [:gss-code :sex :age :year])
                           (ds/rename-columns {:popn :popn-at-risk}))]
    (-> births-ds
        (ds/rename-columns {:births :popn-at-risk})
        (ds/join-rows popn-not-age-0))))

(defn calc-death-rates
  "Calculates historic age specific mortality rates (ASMR).
  Takes datasets with deaths and with population at risk. Calculates
  death rates as deaths divided by population, and returns new dataset
  with death-rate column added."
  [deaths-ds popn-at-risk]
  (-> deaths-ds
      (wds/join popn-at-risk [:gss-code :sex :age :year])
      (wds/add-derived-column :death-rate [:deaths :popn-at-risk]
                              (fn [d p] (wds/safe-divide [d p])))
      (ds/select-columns [:gss-code :sex :age :year :death-rate])))

(defworkflowfn calc-historic-asmr
  "Takes datasets with historic births, deaths, and
  population. Returns a dataset with a column for historic mortality
  rates calculated from the inputs."
  {:witan/name :ccm-mort/calc-historic-asmr
   :witan/version "1.0.0"
   :witan/input-schema {:historic-deaths DeathsSchema
                        :historic-births BirthsSchema
                        :historic-population PopulationSchema}
   :witan/output-schema {:historic-asmr HistASMRSchema}
   :witan/exported? true}
  [{:keys [historic-deaths historic-births historic-population]} _]
  (->> (create-popn-at-risk-death historic-population historic-deaths historic-births)
       (calc-death-rates historic-deaths)
       (hash-map :historic-asmr)))

(defworkflowfn project-asmr-average-fixed
  "Takes a dataset with historic mortality rates, and parameters for
  the number of years of data to average for the calculation, and the
  jumpoff year. Returns a dataset that now includes projected
  mortality rates, projected with the jumpoff year average method (see
  docs)"
  {:witan/name :ccm-mort/project-asmr
   :witan/version "1.0.0"
   :witan/input-schema {:historic-asmr HistASMRSchema}
   :witan/param-schema {:start-yr-avg-mort s/Int :end-yr-avg-mort s/Int}
   :witan/output-schema {:initial-projected-mortality-rates ProjFixedASMRSchema}
   :witan/exported? true}
  [{:keys [historic-asmr]} {:keys [start-yr-avg-mort end-yr-avg-mort]}]
  {:initial-projected-mortality-rates (cf/jumpoffyr-method-average historic-asmr
                                                                   :death-rate
                                                                   :death-rate
                                                                   start-yr-avg-mort
                                                                   end-yr-avg-mort)})

(defworkflowfn project-asmr-average-applynationaltrend
  "Takes a dataset with projected death rates for the future, a dataset with
  historic mortality rates, and parameters for: first projection year, last
  projection year, the scenario to use in the future rates dataset (e.g. :low,
  :principal or :high), the start and end  years of data to average for the calculation,
  and the last year of mortality data. Returns a dataset that now includes projected
  mortality rates, projected with the jumpoff year average method combined with an
  applied national trend (see docs)"
  {:witan/name :ccm-mort/project-asmr-average-applynationaltrend
   :witan/version "1.0.0"
   :witan/input-schema {:historic-asmr HistASMRSchema
                        :future-mortality-trend-assumption NationalTrendsSchema}
   :witan/param-schema {:start-yr-avg-mort (s/constrained s/Int m-utils/year?)
                        :end-yr-avg-mort (s/constrained s/Int m-utils/year?)
                        :first-proj-yr (s/constrained s/Int m-utils/year?)
                        :last-proj-yr (s/constrained s/Int m-utils/year?)
                        :mort-scenario (s/enum :low :principal :high)
                        :mort-last-yr (s/constrained s/Int m-utils/year?)}
   :witan/output-schema {:initial-projected-mortality-rates ProjASMRSchema}
   :witan/exported? false}
  [{:keys [historic-asmr future-mortality-trend-assumption]}
   {:keys [start-yr-avg-mort end-yr-avg-mort first-proj-yr
           last-proj-yr mort-scenario mort-last-yr]}]
  (let [projected-rates-jumpoffyr (cf/jumpoffyr-method-average historic-asmr
                                                               :death-rate
                                                               :death-rate
                                                               start-yr-avg-mort
                                                               end-yr-avg-mort)]
    {:initial-projected-mortality-rates (cf/apply-national-trend projected-rates-jumpoffyr
                                                                 future-mortality-trend-assumption
                                                                 first-proj-yr
                                                                 last-proj-yr
                                                                 mort-scenario
                                                                 :death-rate)}))

(defworkflowfn project-deaths-from-fixed-rates
  "Takes a dataset with population at risk from the current year of the projection
  loop and another dataset with fixed death rates for the population. Returns a
  dataset with a column of deaths, which are the product of popn at risk & death rates"
  {:witan/name :ccm-mort/project-deaths-fixed-rates
   :witan/version "1.0.0"
   :witan/input-schema {:initial-projected-mortality-rates ProjFixedASMRSchema
                        :population-at-risk PopulationAtRiskSchema}
   :witan/output-schema {:deaths DeathsOutputSchema}
   :witan/exported? true}
  [{:keys [initial-projected-mortality-rates population-at-risk]} _]
  {:deaths
   (cf/project-component-fixed-rates population-at-risk
                                     initial-projected-mortality-rates
                                     :death-rate :deaths)})

(defworkflowfn project-deaths
  "Takes the current year of the projection, a dataset with population at risk from that year,
  and another dataset with death rates for the population for all projection years. Death
  rates are filtered for the current year. Returns a dataset with a column of deaths, which are
  the product of popn at risk & death rates"
  {:witan/name :ccm-mort/project-deaths
   :witan/version "1.0.0"
   :witan/input-schema {:initial-projected-mortality-rates ProjASMRSchema
                        :population-at-risk PopulationAtRiskSchema
                        :loop-year (s/constrained s/Int m-utils/year?)}
   :witan/output-schema  {:deaths DeathsOutputSchema}
   :witan/exported? true}
  [{:keys [initial-projected-mortality-rates population-at-risk loop-year]} _]
  {:deaths
   (cf/project-component population-at-risk
                         initial-projected-mortality-rates
                         loop-year
                         :death-rate :deaths)})
