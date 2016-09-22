(ns witan.models.dem.ccm.core.projection-loop
  (:require [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [witan.workspace-api :refer [defworkflowfn defworkflowpred
                                         defworkflowoutput]]
            [witan.workspace-api.utils :as utils]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [witan.models.dem.ccm.fert.fertility :as fert]
            [witan.models.dem.ccm.mort.mortality :as mort]
            [witan.models.dem.ccm.mig.migration :as mig]
            [witan.models.dem.ccm.models-utils :as m-utils]
            [taoensso.timbre :as timbre]))

(defn create-empty-ds
  [schema]
  (ds/dataset (map (comp :v :schema) (:column-names schema)) []))

(defworkflowpred finished-looping?
  {:witan/name :ccm-core/ccm-loop-pred
   :witan/version "1.0.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/param-schema {:last-proj-year YearSchema}
   :witan/exported? true}
  [{:keys [population]} {:keys [last-proj-year]}]
  (let [loop-year (m-utils/get-last-year population)]
    (>= loop-year last-proj-year)))

(defworkflowfn prepare-inputs-1-0-0
  "Step happening before the projection loop.
   Takes in the historic population and outputs the latest year and
   the population for that year which will be updated within the
   projection loop. Also outputs the the population that will ultimately
   contain the historic population and the population projections."
  {:witan/name :ccm-core/prepare-inputs
   :witan/version "1.0.0"
   :witan/param-schema {:first-proj-year YearSchema}
   :witan/input-schema {:historic-population PopulationSchema}
   :witan/output-schema {:population PopulationSchema
                         :aggregated-net-migration NetMigrationSchema
                         :aggregated-births BirthsSchema
                         :aggregated-deaths DeathsOutputSchema}
   :witan/exported? true}
  [{:keys [historic-population]} {:keys [first-proj-year]}]
  {:population (wds/select-from-ds historic-population {:year (dec first-proj-year)})
   :aggregated-births (create-empty-ds BirthsSchema)
   :aggregated-deaths (create-empty-ds DeathsOutputSchema)
   :aggregated-net-migration (create-empty-ds NetMigrationSchema)})

(defworkflowfn select-starting-popn
  "Takes in a dataset of popn estimates.
   Returns a dataset of the starting population for the next year's projection."
  {:witan/name :ccm-core/select-starting-popn
   :witan/version "1.0.0"
   :witan/input-schema {:population PopulationSchema
                        :aggregated-net-migration NetMigrationSchema
                        :aggregated-births BirthsSchema
                        :aggregated-deaths DeathsOutputSchema}
   :witan/output-schema {:latest-year-popn PopulationSchema
                         :population PopulationSchema
                         :aggregated-net-migration NetMigrationSchema
                         :aggregated-births BirthsSchema
                         :aggregated-deaths DeathsOutputSchema
                         :population-at-risk PopulationAtRiskSchema}
   :witan/exported? true}
  [{:keys [population aggregated-births aggregated-deaths aggregated-net-migration]} _]
  (let [last-year (m-utils/get-last-year population)
        latest-year-popn (wds/select-from-ds population {:year last-year})
        updated-year (ds/emap-column latest-year-popn :year inc)]
    {:latest-year-popn updated-year
     :population-at-risk (ds/rename-columns updated-year {:popn :popn-at-risk})
     :population population
     :aggregated-net-migration aggregated-net-migration
     :aggregated-births aggregated-births
     :aggregated-deaths aggregated-deaths}))

(defworkflowfn age-on
  "Takes in a dataset with the starting-population.
   Returns a dataset where the population is aged on 1 year.
   Last year's 89 year olds are aged on and added to this year's
   90+ age group (represented in code as age 90)"
  {:witan/name :ccm-core/age-on
   :witan/version "1.0.0"
   :witan/input-schema {:latest-year-popn PopulationSchema}
   :witan/output-schema {:latest-year-popn PopulationSchema}
   :witan/exported? true}
  [{:keys [latest-year-popn]} _]
  (let [aged-on (-> latest-year-popn
                    (ds/emap-column :age (fn [v] (if (< v 90) (inc v) v)))
                    (wds/rollup :sum :popn [:gss-code :sex :age :year]))]
    {:latest-year-popn aged-on}))

(defworkflowfn add-births
  "Takes in a dataset of aged on popn and dataset of births by sex & gss code.
   Returns a dataset where the births output from the fertility module is
   appended to the aged-on population, adding age groups 0."
  {:witan/name :ccm-core/add-births
   :witan/version "1.0.0"
   :witan/input-schema {:latest-year-popn PopulationSchema :births BirthsBySexSchema}
   :witan/output-schema {:latest-year-popn PopulationSchema}
   :witan/exported? true}
  [{:keys [latest-year-popn births]} _]
  (let [births-n (wds/row-count births)
        aged-on-popn-with-births (-> births
                                     (ds/rename-columns {:births :popn})
                                     (ds/join-rows latest-year-popn)
                                     (ds/select-columns [:gss-code :sex :age :year :popn]))]
    {:latest-year-popn aged-on-popn-with-births}))

(defworkflowfn remove-deaths
  "Takes in a dataset of aged on popn with births added, and a dataset
   of deaths by sex.
   Returns a dataset where the deaths output from the mortality module is
   subtracted from the popn dataset."
  {:witan/name :ccm-core/remove-deaths
   :witan/version "1.0.0"
   :witan/input-schema {:latest-year-popn PopulationSchema :deaths DeathsOutputSchema}
   :witan/output-schema {:latest-year-popn PopulationSchema}
   :witan/exported? true}
  [{:keys [latest-year-popn deaths]} _]
  (let [survived-popn (-> deaths
                          (ds/select-columns [:gss-code :sex :age :deaths])
                          (wds/join latest-year-popn [:gss-code :sex :age])
                          (wds/add-derived-column :popn-survived [:popn :deaths] -)
                          (ds/select-columns [:gss-code :sex :age :year :popn-survived])
                          (ds/rename-columns {:popn-survived :popn}))]
    {:latest-year-popn survived-popn}))

(defworkflowfn apply-migration
  "Takes in a dataset of popn estimates for the current year and a
   dataset with the net migrants for the same year.
   Returns a dataset where the migrants are added to the popn dataset"
  {:witan/name :ccm-core/apply-migration
   :witan/version "1.0.0"
   :witan/input-schema {:latest-year-popn PopulationSchema :net-migration NetMigrationSchema}
   :witan/output-schema {:latest-year-popn PopulationSchema}
   :witan/exported? true}
  [{:keys [latest-year-popn net-migration]} _]
  (let [popn-w-migrants (-> latest-year-popn
                            (wds/join net-migration [:gss-code :sex :age])
                            (wds/add-derived-column :popn-migrated [:popn :net-mig] +)
                            (ds/select-columns [:gss-code :sex :age :year :popn-migrated])
                            (ds/rename-columns {:popn-migrated :popn}))]
    {:latest-year-popn popn-w-migrants}))

(defworkflowfn finalise-popn-1-0-0
  "Performs finalisation of the population data"
  {:witan/name :ccm-core/finalise-popn
   :witan/version "1.0.0"
   :witan/input-schema {:latest-year-popn PopulationSchema}
   :witan/output-schema {:final-popn PopulationSchema}}
  [{:keys [latest-year-popn]} _]
  {:final-popn latest-year-popn})

(defworkflowfn append-by-year-1-0-0
  "Takes in a dataset of population for previous years and a dataset of
   projected population for the next year of projection.
   Returns a datasets that appends the second dataset to the first one."
  {:witan/name :ccm-core/append-years
   :witan/version "1.0.0"
   :witan/input-schema {:final-popn PopulationSchema
                        :population PopulationSchema
                        :births BirthsBySexSchema
                        :aggregated-births BirthsBySexSchema
                        :net-migration NetMigrationSchema
                        :aggregated-net-migration NetMigrationSchema
                        :deaths DeathsOutputSchema
                        :aggregated-deaths DeathsOutputSchema}
   :witan/output-schema {:population PopulationSchema
                         :aggregated-births BirthsSchema
                         :aggregated-deaths DeathsOutputSchema
                         :aggregated-net-migration NetMigrationSchema}}
  [{:keys [final-popn population
           births aggregated-births
           deaths aggregated-deaths
           net-migration aggregated-net-migration]} _]
  {:population (ds/join-rows population final-popn)
   :aggregated-births (ds/join-rows aggregated-births births)
   :aggregated-deaths (ds/join-rows aggregated-deaths deaths)
   :aggregated-net-migration (ds/join-rows aggregated-net-migration net-migration)})

(defworkflowoutput ccm-out
  "Returns the population field"
  {:witan/name :ccm-core-out/ccm-out
   :witan/version "1.0.0"
   :witan/input-schema {:population PopulationSchema
                        :aggregated-births BirthsSchema
                        :aggregated-deaths DeathsOutputSchema
                        :aggregated-net-migration NetMigrationSchema}}
  [{:keys [population aggregated-net-migration aggregated-deaths aggregated-births]} _]
  {:births aggregated-births
   :population population
   :net-migration aggregated-net-migration
   :deaths aggregated-deaths})
