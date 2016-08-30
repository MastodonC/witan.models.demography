(ns witan.models.dem.ccm.fert.fertility
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [schema.core :as s]
            [witan.workspace-api.utils :as utils]
            [witan.models.dem.ccm.models-utils :as m-utils]))

(defn create-popn-at-risk-birth
  "Creates a population at risk for calculating historic age specific
  fertility rates. Takes in the historic population and a base year."
  [historic-population base-year]
  (-> historic-population
      (wds/select-from-ds {:year (dec base-year)})
      (ds/rename-columns {:popn :popn-at-risk})))

(defn calc-asfr-birth-data-year
  "Takes the population at risk for the birth data year and ONS
  projections for births by age of mother. Returns fertility
  rates for the birth data year."
  [popn-at-risk-birth-data-year hist-births-by-age-mother]
  (-> hist-births-by-age-mother
      (wds/join popn-at-risk-birth-data-year [:gss-code :sex :age])
      (wds/add-derived-column :births [:births] (fn [b] (or b 0.0)))
      (wds/add-derived-column :fert-rate-birth-data-year [:births :popn-at-risk]
                              (fn [b p] (wds/safe-divide [b p])))
      (ds/select-columns [:gss-code :sex :age :fert-rate-birth-data-year])))

(defn calc-estimated-births
  "Takes in the population at risk for fert-base-year and the dataset
  with asfr for the base year of the birth data, to calculate the
  estimated births which is totalled by gss-code and year."
  [popn-at-risk-fert-base-year asfr-birth-data-year]
  (-> asfr-birth-data-year
      (wds/join popn-at-risk-fert-base-year [:gss-code :sex :age])
      (wds/add-derived-column :births [:popn-at-risk :fert-rate-birth-data-year]
                              (fn [p b] (* ^double p b)))
      (wds/add-derived-column :year [:year] inc)
      (wds/rollup :sum :births [:gss-code :year])
      (ds/rename-columns {:births :estimated-births})))

(defn calc-actual-births
  "Takes in the historic data of total births, filters for
  the base year and totals the number of births for that year
  and gss-code."
  [historic-births fert-base-year]
  (-> historic-births
      (wds/select-from-ds {:year fert-base-year})
      (wds/rollup :sum :births [:gss-code :year])
      (ds/rename-columns {:births :actual-births})))

(defn calc-scaling-factors
  "Takes in the estimated and actual births. Divides them to get
  the scaling factors for each year and gss-code."
  [actual-births estimated-births]
  (-> estimated-births
      (wds/join actual-births [:gss-code :year])
      (wds/add-derived-column :scaling-factor [:actual-births
                                               :estimated-births]
                              (fn [a e] (wds/safe-divide [a e])))
      (ds/select-columns [:gss-code :year :scaling-factor])))

(defn calc-scaled-fert-rates
  "Takes in scaling factor and base fertility rates to
  return the scaled fertility rates."
  [scaling-factors asfr-birth-data-year]
  (-> scaling-factors
      (wds/join asfr-birth-data-year [:gss-code])
      (wds/add-derived-column :fert-rate [:scaling-factor
                                          :fert-rate-birth-data-year]
                              (fn [s b] (* ^double s b)))))

(defworkflowfn calculate-historic-asfr
  "Takes in three datasets: the historic total births, the historic
   population, the base age-specific fertility rate and the base year.
   Returns a dataset containing the historic age specific fertility rates."
  {:witan/name :ccm-fert/calc-hist-asfr
   :witan/version "1.0.0"
   :witan/input-schema {:historic-births-by-age-mother BirthsAgeSexMotherSchema
                        :historic-population PopulationSchema
                        :historic-births BirthsSchema}
   :witan/param-schema {:fert-base-year (s/constrained s/Int m-utils/year?)}
   :witan/output-schema {:historic-asfr HistASFRSchema}
   :witan/exported? true}
  [{:keys [base-asfr historic-births-by-age-mother historic-population historic-births]}
   {:keys [fert-base-year]}]
  (let [birth-data-year (m-utils/get-last-year historic-births-by-age-mother)
        _ (utils/property-holds? birth-data-year m-utils/year? (str birth-data-year " is not a year"))
        popn-at-risk-birth-data-year (-> historic-population
                                         (create-popn-at-risk-birth birth-data-year)
                                         (ds/select-columns
                                          [:gss-code :sex :age :popn-at-risk]))
        asfr-birth-data-year (calc-asfr-birth-data-year popn-at-risk-birth-data-year
                                                        historic-births-by-age-mother)
        popn-at-risk-fert-proj-base-year (create-popn-at-risk-birth historic-population fert-base-year)
        estimated-births (calc-estimated-births popn-at-risk-fert-proj-base-year asfr-birth-data-year)]
    {:historic-asfr
     (-> historic-births
         (calc-actual-births fert-base-year)
         (calc-scaling-factors estimated-births)
         (calc-scaled-fert-rates asfr-birth-data-year)
         (ds/select-columns [:gss-code :sex :age :year :fert-rate]))}))

(defworkflowfn project-asfr-1-0-0
  "Project ASFR finalyearhist fixed. Takes dataset of historic age specific 
   fertility rates, and parameter for the base year of fertility data. 
   Returns dataset with projected age specific fertility rates, calculated 
   using the jumpoff year finalyearhist method (see docs)."
  {:witan/name :ccm-fert/project-asfr
   :witan/version "1.0.0"
   :witan/input-schema {:historic-asfr HistASFRSchema}
   :witan/output-schema {:initial-projected-fertility-rates ProjFixedASFRSchema}
   :witan/exported? true}
  [{:keys [historic-asfr]} _]
  (let [final-year (m-utils/get-last-year historic-asfr)
        _ (utils/property-holds? final-year m-utils/year? (str final-year " is not a year"))
        final-year-hist (wds/select-from-ds historic-asfr {:year final-year})]
    {:initial-projected-fertility-rates
     (ds/select-columns final-year-hist [:gss-code :sex :age :fert-rate])}))

(defn project-asfr
  "Given historic fertility rates, project asfr using specified variant of the projection method.
   Currently there is only 1 year of data so the jumpoff year method is set to finalyearhist. 
   Returns dataset with projected fertility rate in :fert-rate column for all years of projection."
  [{:keys [historic-asfr future-fertility-trend-assumption]}
   {:keys [variant first-proj-year last-proj-year fert-scenario]}]
  (case variant
    :fixed {:initial-projected-fertility-rates
            (-> (cf/jumpoff-year-method-final-year-hist historic-asfr :fert-rate)
                (cf/add-years-to-fixed-methods first-proj-year
                                               last-proj-year)
                (ds/select-columns [:gss-code :sex :age :year :fert-rate]))}
    
    :applynationaltrend (let [projected-rates-jumpoff-year (wds/add-derived-column historic-asfr
                                                                                   :year
                                                                                   [:year]
                                                                                   (fn [y] first-proj-year))]
                          {:initial-projected-fertility-rates
                           (cf/apply-national-trend-fertility projected-rates-jumpoff-year
                                                              future-fertility-trend-assumption
                                                              first-proj-year
                                                              last-proj-year
                                                              fert-scenario
                                                              :fert-rate)})))

(defworkflowfn project-asfr-1-1-0
  "Takes a back series of age-specific fertility rates. For projecting fertility rates in years 
  following the first projection year up until the last projection year: the fixed method applies 
  the jump off year rates each year; the apply national trend method requires a national trend 
  dataset and a scenario to use as the future mortality trend assumption, and generates variable 
  rates for each year. Both methods use the final year of historic data for the jumpoff method.
  Outputs a dataset of projected age-specific fertility rates for each projection year. "
  {:witan/name :ccm-fert/project-asfr
   :witan/version "1.1.0"
   :witan/input-schema {:historic-asfr HistASFRSchema
                        :future-fertility-trend-assumption NationalFertilityTrendsSchema}
   :witan/param-schema {:variant (s/enum :fixed :applynationaltrend)
                        :first-proj-year (s/constrained s/Int m-utils/year?)
                        :last-proj-year (s/constrained s/Int m-utils/year?)
                        :fert-scenario (s/enum :low :principal :high :low-2012 :principal-2012 :high-2012)}
   :witan/output-schema {:initial-projected-fertility-rates ProjASFRSchema}
   :witan/exported? true}
  [inputs params]
  (project-asfr inputs params))

(defworkflowfn project-births-1-0-0
  "Project births from fixed rates. Takes a dataset with population at risk 
   from the current year of the projection loop and another dataset with 
   fixed fertility rates for the population. Returns a dataset with a column 
   of births, which are the product of popn at risk & the rates"
  {:witan/name :ccm-fert/project-births
   :witan/version "1.0.0"
   :witan/input-schema {:initial-projected-fertility-rates ProjFixedASFRSchema
                        :population-at-risk PopulationAtRiskSchema}
   :witan/output-schema {:births-by-age-sex-mother BirthsAgeSexMotherSchema}
   :witan/exported? true}
  [{:keys [initial-projected-fertility-rates population-at-risk]} _]
  (let [projected-births (cf/project-component-fixed-rates
                          population-at-risk
                          initial-projected-fertility-rates
                          :fert-rate :births)]
    {:births-by-age-sex-mother projected-births}))

(defworkflowfn project-births-1-1-0
  "Takes the current year of the projection, a dataset with population at risk from that year,
  and another dataset with birth rates for the population for all projection years. Birth
  rates are filtered for the current year. Returns a dataset with a column of births, which are
  the product of popn at risk & birth rates"
  {:witan/name :ccm-fert/project-births
   :witan/version "1.1.0"
   :witan/input-schema {:initial-projected-fertility-rates ProjASFRSchema
                        :population-at-risk PopulationAtRiskSchema
                        :loop-year (s/constrained s/Int m-utils/year?)}
   :witan/output-schema  {:births-by-age-sex-mother BirthsAgeSexMotherSchema}
   :witan/exported? true}
  [{:keys [initial-projected-fertility-rates population-at-risk loop-year]} _]
  (let [max-year (m-utils/get-last-year initial-projected-fertility-rates)
        _ (utils/property-holds? max-year #(<= loop-year %) #(str % " is less than loop year"))]
    {:births-by-age-sex-mother (cf/project-component population-at-risk
                                                     initial-projected-fertility-rates
                                                     loop-year
                                                     :fert-rate :births)}))

(defn- gather-births-by-sex
  "Given a dataset with columns :gss-code, :m, and :f (where :m and :f are
   male and female births), gathers births data into :births column and
   sex into :sex column, returning a new dataset. Standing for a universal
   gather function in witan.datasets similar to gather in tidyR"
  [births-by-mf]
  (let [births-f (-> births-by-mf
                     (ds/add-column :sex ["F"])
                     (ds/rename-columns {:f :births})
                     (ds/select-columns [:gss-code :sex :births]))
        births-m (-> births-by-mf
                     (ds/add-column :sex ["M"])
                     (ds/rename-columns {:m :births})
                     (ds/select-columns [:gss-code :sex :births]))]
    (ds/join-rows births-m births-f)))

(defworkflowfn combine-into-births-by-sex
  "Takes dataset of historic age specific fertility rates, and parameter
   for the base year of fertility data. Returns dataset with projected
   age specific fertility rates, calculated using the jumpoff year average
   method (see docs)."
  {:witan/name :ccm-fert/combine-into-births-by-sex
   :witan/version "1.0.0"
   :witan/input-schema {:births-by-age-sex-mother BirthsAgeSexMotherSchema}
   :witan/param-schema {:proportion-male-newborns double}
   :witan/output-schema {:births BirthsBySexSchema}
   :witan/exported? true}
  [{:keys [births-by-age-sex-mother]} {:keys [proportion-male-newborns]}]
  (let [births-by-sex (-> births-by-age-sex-mother
                          (wds/rollup :sum :births [:gss-code])
                          (wds/add-derived-column :m [:births]
                                                  (fn [b] (double (* proportion-male-newborns b))))
                          (wds/add-derived-column :f [:births]
                                                  (fn [b] (double
                                                           (* (- 1 proportion-male-newborns) b))))
                          (ds/select-columns [:gss-code :m :f])
                          gather-births-by-sex)]
    {:births births-by-sex}))
