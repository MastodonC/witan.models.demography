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
  (let [max-year-deaths (m-utils/get-last-year deaths-ds)
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

;;Age specific fertility rates projection
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Note: Fertility & mortality use different project ASR functions because future trends data ;;
;; from ONS is different. Mortality future trend has a sex column whereas fertility future    ;;
;; trend does not. Also, the future rates dataset needs to be aged on for mortality in the    ;;
;; calc-proportional-differences function due to the way ages are used by the ONS, to give    ;;
;; the correct death rates. This is not the case for the future fertility rates.              ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn calc-proportional-differences-mortality
  "Takes a dataset of projected death rates and returns a dataset with the proportional
   difference in the rate between each year and the previous year. Also takes first
   and last years of projection, and keyword with the scenario :low, :principal or :high."
  [future-rates-trend-assumption first-proj-year last-proj-year scenario]
  (-> future-rates-trend-assumption
      (wds/select-from-ds {:year {:$gte first-proj-year :$lte last-proj-year}})
      (ds/select-columns [:sex :age :year scenario])
      (ds/rename-columns {scenario :national-trend})
      (cf/order-ds [:sex :age :year])
      (as-> rates (ds/add-column rates :last-year-nt (cf/lag rates :national-trend)))
      (wds/add-derived-column :age [:age] inc)
      (wds/add-derived-column :prop-diff [:national-trend :last-year-nt :year]
                              (fn [national-trend last-year-nt year]
                                (if (== year first-proj-year)
                                  0
                                  (wds/safe-divide [(- national-trend last-year-nt) last-year-nt]))))
      (ds/select-columns [:sex :age :year :prop-diff])))

(defn apply-national-trend-mortality
  "Takes a dataset of projected rates or values for the jumpoff year, and a dataset of
   projected national rates for the following years of the projection (currently
   this is ONS data). Takes parameters for first & last projection years, the
   scenario to be used from the future rates dataset (e.g. :low, :principal, or
   :high), and the name of the column with rates or value in the jumpoff year dataset
   (e.g. :death-rate or :deaths). Returns a dataset with projected rates or values
   for each age, sex, and projection year, calculated by applying the trend from
   the projected national rates to the data from the jumpoff year dataset."
  [jumpoff-year-projection future-assumption
   first-proj-year last-proj-year scenario assumption-col]
  (let [proportional-differences (calc-proportional-differences-mortality future-assumption
                                                                          first-proj-year
                                                                          last-proj-year
                                                                          scenario)
        jumpoff-year-projection-n (wds/row-count jumpoff-year-projection)
        projection-first-proj-year (-> jumpoff-year-projection
                                       (ds/add-column :year (repeat jumpoff-year-projection-n
                                                                    first-proj-year))
                                       (ds/select-columns [:gss-code :sex :age :year assumption-col]))]
    (cf/order-ds (:accumulator
                  (reduce (fn [{:keys [accumulator last-calculated]} current-year]
                            (let [projection-this-year (-> proportional-differences
                                                           (wds/select-from-ds {:year current-year})
                                                           (wds/join last-calculated [:sex :age])
                                                           (wds/add-derived-column assumption-col
                                                                                   [assumption-col :prop-diff]
                                                                                   (fn [assumption-col prop-diff]
                                                                                     (* ^double assumption-col (inc prop-diff))))
                                                           (wds/add-derived-column :year [:year] (fn [_] current-year))
                                                           (ds/select-columns [:gss-code :sex :age :year assumption-col]))]
                              {:accumulator (ds/join-rows accumulator projection-this-year)
                               :last-calculated projection-this-year}))
                          {:accumulator projection-first-proj-year :last-calculated projection-first-proj-year}
                          (range (inc first-proj-year) (inc last-proj-year))))
                 [:sex :year :age])))

(defn project-asmr-internal [{:keys [historic-asmr future-mortality-trend-assumption]}
                             {:keys [start-year-avg-mort end-year-avg-mort mort-variant first-proj-year
                                     last-proj-year mort-scenario]}]
  (case mort-variant
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
                                   (apply-national-trend-mortality projected-rates-jumpoff-year
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
                                 (apply-national-trend-mortality projected-rates-jumpoff-year
                                                                 future-mortality-trend-assumption
                                                                 first-proj-year
                                                                 last-proj-year
                                                                 mort-scenario
                                                                 :death-rate)})))

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
                        :mort-variant (s/enum :average-fixed :trend-fixed
                                              :average-applynationaltrend :trend-applynationaltrend)
                        :first-proj-year (s/constrained s/Int m-utils/year?)
                        :last-proj-year (s/constrained s/Int m-utils/year?)
                        :mort-scenario (s/enum :low :principal :high)}
   :witan/output-schema {:initial-projected-mortality-rates ProjASMRSchema}
   :witan/exported? true}
  [inputs params]
  (project-asmr-internal inputs params))

(defworkflowfn project-deaths-1-1-0
  "Takes the current year of the projection, a dataset with population at risk from that year,
  and another dataset with death rates for the population for all projection years. Death
  rates are filtered for the current year. Returns a dataset with a column of deaths, which are
  the product of popn at risk & death rates"
  {:witan/name :ccm-mort/project-deaths
   :witan/version "1.1.0"
   :witan/input-schema {:initial-projected-mortality-rates ProjASMRSchema
                        :population-at-risk PopulationAtRiskSchema
                        :loop-year (s/constrained s/Int m-utils/year?)}
   :witan/output-schema  {:deaths DeathsOutputSchema}
   :witan/exported? true}
  [{:keys [initial-projected-mortality-rates population-at-risk loop-year]} _]
  (let [max-year (m-utils/get-last-year initial-projected-mortality-rates)
        _ (utils/property-holds? max-year #(<= loop-year %) #(str % " is less than loop year"))]
    {:deaths (cf/project-component population-at-risk
                                   initial-projected-mortality-rates
                                   loop-year
                                   :death-rate :deaths)}))
