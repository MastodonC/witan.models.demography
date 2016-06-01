(ns witan.models.dem.ccm.core.projection-loop
  (:require [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]))

(defworkflowfn keep-looping?
  {:witan/name :ccm-core/ccm-loop-pred
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema :loop-year s/Int}
   :witan/param-schema {:last-proj-year s/Int}
   :witan/output-schema {:loop-predicate s/Bool}
   :witan/exported? true}
  [{:keys [loop-year]} {:keys [last-proj-year]}]
  {:loop-predicate (< loop-year last-proj-year)})

(defworkflowfn prepare-inputs
  {:witan/name :ccm-core/get-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:population PopulationSchema}
   :witan/output-schema {:loop-year s/Int :latest-yr-popn PopulationSchema}}
  [{:keys [population]} _]
  (let [last-yr (reduce max (ds/column population :year))
        last-yr-popn (i/query-dataset population {:year last-yr})]
    {:loop-year last-yr :latest-yr-popn last-yr-popn}))

(defworkflowfn select-starting-popn
  "Takes in a dataset of popn estimates.
   Returns a dataset of the starting population for the next year's projection."
  {:witan/name :ccm-core/get-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema :population PopulationSchema
                        :loop-year s/Int}
   :witan/output-schema {:latest-yr-popn PopulationSchema :loop-year s/Int}}
  [{:keys [latest-yr-popn population loop-year]} _]
  (let [update-yr (ds/emap-column latest-yr-popn :year inc)]
    {:latest-yr-popn update-yr :loop-year (inc loop-year)}))

(defworkflowfn age-on
  "Takes in a dataset with the starting-population.
   Returns a dataset where the population is aged on 1 year.
   Last year's 89 year olds are aged on and added to this year's
   90+ age group (represented in code as age 90)"
  {:witan/name :ccm-cor/age-on
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema}
   :witan/output-schema {:latest-yr-popn PopulationSchema}}
  [{:keys [latest-yr-popn]} _]
  (let [aged-on (-> latest-yr-popn
                    (ds/emap-column :age (fn [v] (if (< v 90) (inc v) v)))
                    (wds/rollup :sum :popn [:gss-code :sex :age :year]))]
    {:latest-yr-popn aged-on}))

(defworkflowfn add-births
  "Takes in a dataset of aged on popn and dataset of births by sex & gss code.
   Returns a dataset where the births output from the fertility module is
   appended to the aged-on population, adding age groups 0."
  {:witan/name :ccm-core/add-births
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema :births BirthsBySexSchema
                        :loop-year s/Int}
   :witan/output-schema {:latest-yr-popn PopulationSchema}}
  [{:keys [latest-yr-popn births loop-year]} _]
  (let [aged-on-popn-with-births (-> births
                                     (ds/add-column :age (repeat 0))
                                     (ds/add-column :year (repeat loop-year))
                                     (ds/rename-columns {:births :popn})
                                     (ds/join-rows latest-yr-popn)
                                     (ds/select-columns [:gss-code :sex :age :year :popn]))]
    {:latest-yr-popn aged-on-popn-with-births}))

(defworkflowfn remove-deaths
  "Takes in a dataset of aged on popn with births added, and a dataset
   of deaths by sex.
   Returns a dataset where the deaths output from the mortality module is
   subtracted from the popn dataset."
  {:witan/name :ccm-core/remove-deaths
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema :deaths DeathsOutputSchema}
   :witan/output-schema {:latest-yr-popn PopulationSchema}}
  [{:keys [latest-yr-popn deaths]} _]
  (let [survived-popn (-> deaths
                          (ds/select-columns [:gss-code :sex :age :deaths])
                          (wds/join latest-yr-popn [:gss-code :sex :age])
                          (wds/add-derived-column :popn-survived [:popn :deaths] -)
                          (ds/select-columns [:gss-code :sex :age :year :popn-survived])
                          (ds/rename-columns {:popn-survived :popn}))]
    {:latest-yr-popn survived-popn}))

(defworkflowfn apply-migration
  "Takes in a dataset of popn estimates for the current year and a
   dataset with the net migrants for the same year.
   Returns a dataset where the migrants are added to the popn dataset"
  {:witan/name :ccm-core/apply-migration
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema :net-migration NetMigrationSchema}
   :witan/output-schema {:latest-yr-popn PopulationSchema}}
  [{:keys [latest-yr-popn net-migration]} _]
  (let [popn-w-migrants (-> latest-yr-popn
                            (wds/join net-migration [:gss-code :sex :age])
                            (wds/add-derived-column :popn-migrated [:popn :net-mig] +)
                            (ds/select-columns [:gss-code :sex :age :year :popn-migrated])
                            (ds/rename-columns {:popn-migrated :popn}))]
    {:latest-yr-popn popn-w-migrants}))

(defworkflowfn join-popn-latest-yr
  "Takes in a dataset of popn for previous years and a dataset of
   projected population for the next year of projection.
   Returns a datasets that appends the second dataset to the first one."
  {:witan/name :ccm-core/join-yrs
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn PopulationSchema :population PopulationSchema}
   :witan/output-schema {:population PopulationSchema}}
  [{:keys [latest-yr-popn population]} _]
  {:population (ds/join-rows population latest-yr-popn)})

(defn looping-test
  [inputs params]
  (let [prepared-inputs (prepare-inputs inputs)]
    (loop [inputs prepared-inputs]
      (let [inputs' (-> inputs
                        select-starting-popn
                        age-on
                        add-births
                        remove-deaths
                        apply-migration
                        join-popn-latest-yr)]
        (println (format "Projecting for year %d..." (:loop-year inputs')))
        (if (:loop-predicate (keep-looping? inputs' params))
          (recur inputs')
          (:population inputs'))))))
