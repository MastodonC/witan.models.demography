(ns witan.models.dem.ccm.fert.fertility
  (:require [witan.models.dem.ccm.components-functions :as cf]
            [witan.workspace-api :refer [defworkflowfn]]
            [clojure.core.matrix.dataset :as ds]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [incanter.core :as i]
            [schema.core :as s]))

(defn create-popn-at-risk-birth
  "Creates a population at risk for calculating historic age specific
  fertility rates. Takes in the historic population and a base year."
  [historic-population base-year]
  (-> historic-population
      (i/query-dataset {:year (dec base-year)})
      (ds/rename-columns {:popn :popn-at-risk})))

(defn calc-asfr-birth-data-yr
  "Takes the population at risk for the birth data year and ONS
  projections for births by age of mother. Returns fertility
  rates for the birth data year."
  [popn-at-risk-birth-data-yr ons-proj-births-by-age-mother]
  (-> ons-proj-births-by-age-mother
      (wds/join popn-at-risk-birth-data-yr [:gss-code :sex :age])
      (wds/add-derived-column :births [:births] (fn [b] (or b 0.0)))
      (wds/add-derived-column :fert-rate-birth-data-yr [:births :popn-at-risk]
                              (fn [b p] (wds/safe-divide [b p])))
      (ds/select-columns [:gss-code :sex :age :fert-rate-birth-data-yr])))

(defn calc-estimated-births
  "Takes in the population at risk for fert-base-yr and the dataset
  with asfr for the base year of the birth data, to calculate the
  estimated births which is totalled by gss-code and year."
  [popn-at-risk-fert-base-yr asfr-birth-data-yr]
  (-> asfr-birth-data-yr
      (wds/join popn-at-risk-fert-base-yr [:gss-code :sex :age])
      (wds/add-derived-column :births [:popn-at-risk :fert-rate-birth-data-yr]
                              (fn [p b] (* ^double p b)))
      (wds/add-derived-column :year [:year] inc)
      (wds/rollup :sum :births [:gss-code :year])
      (ds/rename-columns {:births :estimated-births})))

(defn calc-actual-births
  "Takes in the historic data of total births, filters for
  the base year and totals the number of births for that year
  and gss-code."
  [historic-births fert-base-yr]
  (-> historic-births
      (i/query-dataset {:year fert-base-yr})
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
  [scaling-factors asfr-birth-data-yr]
  (-> scaling-factors
      (wds/join asfr-birth-data-yr [:gss-code])
      (wds/add-derived-column :fert-rate [:scaling-factor
                                          :fert-rate-birth-data-yr]
                              (fn [s b] (* ^double s b)))))

(defworkflowfn calculate-historic-asfr
  "Takes in three datasets: the historic total births, the historic
   population, the base age-specific fertility rate and the base year.
   Returns a dataset containing the historic age specific fertility rates."
  {:witan/name :ccm-fert/calc-hist-asfr
   :witan/version "1.0.0"
   :witan/input-schema {:ons-proj-births-by-age-mother BirthsAgeSexMotherSchema
                        :historic-population PopulationSchema
                        :historic-births BirthsSchema}
   :witan/param-schema {:fert-base-yr s/Int}
   :witan/output-schema {:historic-asfr HistASFRSchema}
   :witan/exported? true}
  [{:keys [base-asfr ons-proj-births-by-age-mother historic-population historic-births]}
   {:keys [fert-base-yr]}]
  (let [birth-data-yr (reduce max (ds/column
                                   ons-proj-births-by-age-mother :year))
        popn-at-risk-birth-data-yr (-> historic-population
                                       (create-popn-at-risk-birth birth-data-yr)
                                       (ds/select-columns
                                        [:gss-code :sex :age :popn-at-risk]))
        asfr-birth-data-yr (calc-asfr-birth-data-yr popn-at-risk-birth-data-yr
                                                    ons-proj-births-by-age-mother)
        popn-at-risk-fert-proj-base-yr (create-popn-at-risk-birth historic-population fert-base-yr)
        estimated-births (calc-estimated-births popn-at-risk-fert-proj-base-yr asfr-birth-data-yr)]
    {:historic-asfr
     (-> historic-births
         (calc-actual-births fert-base-yr)
         (calc-scaling-factors estimated-births)
         (calc-scaled-fert-rates asfr-birth-data-yr)
         (ds/select-columns [:gss-code :sex :age :year :fert-rate]))}))

(defworkflowfn project-asfr-finalyrhist-fixed
  "Takes dataset of historic age specific fertility rates, and parameter
   for the base year of fertility data. Returns dataset with projected
   age specific fertility rates, calculated using the jumpoff year average
   method (see docs)."
  {:witan/name :ccm-fert/project-asfr-finalyrhist-fixed
   :witan/version "1.0.0"
   :witan/input-schema {:historic-asfr HistASFRSchema}
   :witan/output-schema {:initial-projected-fertility-rates ProjFixedASFRSchema}
   :witan/exported? true}
  [{:keys [historic-asfr]} _]
  (let [final-yr (reduce max (ds/column historic-asfr :year))
        final-yr-hist (i/query-dataset historic-asfr {:year final-yr})]
    {:initial-projected-fertility-rates
     (ds/select-columns final-yr-hist [:gss-code :sex :age :fert-rate])}))

(defworkflowfn project-births-from-fixed-rates
  "Takes a dataset with population at risk from the current year of the projection
  loop and another dataset with fixed fertility rates for the population. Returns a
  dataset with a column of births, which are the product of popn at risk & the rates"
  {:witan/name :ccm-fert/project-births-fixed-rates
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

(defn- gather-births-by-sex
  "Given a dataset with columns :gss-code, :m, and :f (where :m and :f are
   male and female births), gathers births data into :births column and
   sex into :sex column, returning a new dataset. Standing for a universal
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
