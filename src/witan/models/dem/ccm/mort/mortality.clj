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
  (let [max-year-deaths (reduce max (ds/column deaths-ds :year))
        _ (utils/property-holds? max-year-deaths m-utils/year? (str max-year-deaths " is not a year"))
        popn-not-age-0 (-> popn-ds
                           (ds/emap-column :year inc)
                           (wds/select-from-ds {:year {:$lte max-year-deaths}})
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

(defn project-asmr [{:keys [historic-asmr future-mortality-trend-assumption]}
                    {:keys [start-year-avg-mort end-year-avg-mort variant first-proj-year
                            last-proj-year mort-scenario]}]
  (case variant
    :average-fixed {:initial-projected-mortality-rates
                    (-> (cf/jumpoff-year-method-average historic-asmr
                                                        :death-rate
                                                        :death-rate
                                                        start-year-avg-mort
                                                        end-year-avg-mort)
                        (cf/add-years-to-fixed-methods first-proj-year
                                                       last-proj-year)
                        (ds/select-columns [:gss-code :sex :age :year :death-rate]))}
    :trend-fixed {:initial-projected-mortality-rates
                  (-> (cf/jumpoff-year-method-trend historic-asmr
                                                    :death-rate
                                                    :death-rate
                                                    start-year-avg-mort
                                                    end-year-avg-mort)
                      (cf/add-years-to-fixed-methods first-proj-year
                                                     last-proj-year)
                      (ds/select-columns [:gss-code :sex :age :year :death-rate]))}
    :average-applynationaltrend (let [projected-rates-jumpoff-year
                                      (cf/jumpoff-year-method-average historic-asmr
                                                                      :death-rate
                                                                      :death-rate
                                                                      start-year-avg-mort
                                                                      end-year-avg-mort)]
                                  {:initial-projected-mortality-rates
                                   (cf/apply-national-trend projected-rates-jumpoff-year
                                                            future-mortality-trend-assumption
                                                            first-proj-year
                                                            last-proj-year
                                                            mort-scenario
                                                            :death-rate)})
    :trend-applynationaltrend (let [projected-rates-jumpoff-year
                                    (cf/jumpoff-year-method-trend historic-asmr
                                                                  :death-rate
                                                                  :death-rate
                                                                  start-year-avg-mort
                                                                  end-year-avg-mort)]
                                {:initial-projected-mortality-rates
                                 (cf/apply-national-trend projected-rates-jumpoff-year
                                                          future-mortality-trend-assumption
                                                          first-proj-year
                                                          last-proj-year
                                                          mort-scenario
                                                          :death-rate)})))

(defworkflowfn project-asmr-1-0-0
  "Takes a dataset with historic mortality rates, and parameters for
  the number of years of data to average for the calculation, and the
  jumpoff year. Returns a dataset that now includes projected
  mortality rates, projected with the jumpoff year average method (see
  docs)"
  {:witan/name :ccm-mort/project-asmr
   :witan/version "1.0.0"
   :witan/input-schema {:historic-asmr HistASMRSchema
                        :future-mortality-trend-assumption NationalTrendsSchema}
   :witan/param-schema {:start-year-avg-mort s/Int
                        :end-year-avg-mort s/Int
                        :last-proj-year s/Int
                        :first-proj-year s/Int}
   :witan/output-schema {:initial-projected-mortality-rates ProjASMRSchema}
   :witan/exported? true}
  [inputs params]
  (project-asmr inputs (assoc params
                              :variant :average-fixed
                              :mort-scenario :principal)))

(defworkflowfn project-asmr-1-1-0
  "Takes a back series of age-specific mortality rates and a start and end year on which to
  base calculation of the projected mortality rates in the first loop projection year from
  the historic mortality rates (method dependent on average or trend jump off year method
  variant). For projecting mortality rates in years following the first projection year up
  until the last projection year: the fixed method applies the jump off year rates each
  year; the apply national trend method requires a national trend dataset and a scenario to
  use as the future mortality trend assumption, and generates variable rates for each year.
  Outputs a dataset of projected age-specific mortality rates for each projection year"
  {:witan/name :ccm-mort/project-asmr
   :witan/version "1.1.0"
   :witan/input-schema {:historic-asmr HistASMRSchema
                        :future-mortality-trend-assumption NationalTrendsSchema}
   :witan/param-schema {:start-year-avg-mort s/Int
                        :end-year-avg-mort s/Int
                        :variant (s/enum :average-fixed :trend-fixed
                                         :average-applynationaltrend :trend-applynationaltrend)
                        :first-proj-year (s/constrained s/Int m-utils/year?)
                        :last-proj-year (s/constrained s/Int m-utils/year?)
                        :mort-scenario (s/enum :low :principal :high)}
   :witan/output-schema {:initial-projected-mortality-rates ProjASMRSchema}
   :witan/exported? true}
  [inputs params]
  (project-asmr inputs params))

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
  (let [max-year (reduce max (ds/column initial-projected-mortality-rates :year))
        _ (utils/property-holds? max-year #(<= loop-year %) #(str % " is less than loop year"))]
    {:deaths (cf/project-component population-at-risk
                                   initial-projected-mortality-rates
                                   loop-year
                                   :death-rate :deaths)}))
