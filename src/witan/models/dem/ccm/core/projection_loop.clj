(ns witan.models.dem.ccm.core.projection-loop
  (:require [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn defworkflowpred]]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.schemas :refer :all]
            [witan.models.dem.ccm.fert.fertility-mvp :as fert]
            [witan.models.dem.ccm.mort.mortality-mvp :as mort]
            [witan.models.dem.ccm.mig.net-migration :as mig]
            [taoensso.timbre :as timbre
             :refer (log  trace  debug  info  warn  error  fatal  report
                          logf tracef debugf infof warnf errorf fatalf reportf
                          spy get-env log-env)]))

(defworkflowfn keep-looping?
  {:witan/name :ccm-core/ccm-loop-pred
   :witan/version "1.0"
   :witan/input-schema {:historic-population HistPopulationSchema :loop-year s/Int}
   :witan/param-schema {:last-proj-year s/Int}
   :witan/output-schema {:loop-predicate s/Bool}
   :witan/exported? true}
  [{:keys [loop-year]} {:keys [last-proj-year]}]
  {:loop-predicate (< loop-year last-proj-year)})

(defworkflowpred finished-looping?
  {:witan/name :ccm-core/ccm-loop-pred
   :witan/version "1.0"
   :witan/input-schema {:historic-population HistPopulationSchema :loop-year s/Int}
   :witan/param-schema {:last-proj-year s/Int}
   :witan/exported? true}
  [{:keys [loop-year]} {:keys [last-proj-year]}]
  (> loop-year last-proj-year))

(defworkflowfn prepare-inputs
  "Step happening before the projection loop.
   Takes in the historic population and outputs
   the latest year and the population for that year.
   Both those elements will be updated within the
   projection loop."
  {:witan/name :ccm-core/prepare-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:historic-population HistPopulationSchema}
   :witan/output-schema {:loop-year s/Int :latest-yr-popn HistPopulationSchema}}
  [{:keys [historic-population]} _]
  (let [last-yr (reduce max (ds/column historic-population :year))
        last-yr-popn (i/query-dataset historic-population {:year last-yr})]
    {:loop-year last-yr :latest-yr-popn last-yr-popn}))

(defworkflowfn select-starting-popn
  "Takes in a dataset of popn estimates.
   Returns a dataset of the starting population for the next year's projection."
  {:witan/name :ccm-core/select-starting-popn
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn HistPopulationSchema
                        :loop-year s/Int}
   :witan/output-schema {:latest-yr-popn HistPopulationSchema :loop-year s/Int
                         :population-at-risk HistPopulationSchema}}
  [{:keys [latest-yr-popn loop-year]} _]
  (let [update-yr (ds/emap-column latest-yr-popn :year inc)]
    {:latest-yr-popn update-yr :loop-year (inc loop-year)
     :population-at-risk update-yr}))

(defworkflowfn age-on
  "Takes in a dataset with the starting-population.
   Returns a dataset where the population is aged on 1 year.
   Last year's 89 year olds are aged on and added to this year's
   90+ age group (represented in code as age 90)"
  {:witan/name :ccm-cor/age-on
   :witan/version "1.0"
   :witan/input-schema {:latest-yr-popn HistPopulationSchema}
   :witan/output-schema {:latest-yr-popn HistPopulationSchema}}
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
   :witan/input-schema {:latest-yr-popn HistPopulationSchema :births BirthsBySexSchema
                        :loop-year s/Int}
   :witan/output-schema {:latest-yr-popn HistPopulationSchema}}
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
   :witan/input-schema {:latest-yr-popn HistPopulationSchema :deaths DeathsOutputSchema}
   :witan/output-schema {:latest-yr-popn HistPopulationSchema}}
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
   :witan/input-schema {:latest-yr-popn HistPopulationSchema :net-migration NetMigrationSchema}
   :witan/output-schema {:latest-yr-popn HistPopulationSchema}}
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
   :witan/input-schema {:latest-yr-popn HistPopulationSchema
                        :historic-population HistPopulationSchema}
   :witan/output-schema {:historic-population HistPopulationSchema}}
  [{:keys [latest-yr-popn historic-population]} _]
  {:historic-population (ds/join-rows historic-population latest-yr-popn)})

(defn looping-test
  [inputs params]
  (let [prepared-inputs (-> inputs
                            prepare-inputs
                            (fert/fertility-pre-projection params)
                            (mort/mortality-pre-projection params)
                            (mig/migration-pre-projection params))]
    (loop [inputs prepared-inputs]
      (let [inputs' (-> inputs
                        select-starting-popn
                        (fert/births-projection params)
                        (mort/project-deaths-from-fixed-rates)
                        age-on
                        add-births
                        remove-deaths
                        apply-migration
                        join-popn-latest-yr)]
        (println (format "Projecting for year %d..." (:loop-year inputs')))
        (if (:loop-predicate (keep-looping? inputs' params))
          (recur inputs')
          (:historic-population inputs'))))))
