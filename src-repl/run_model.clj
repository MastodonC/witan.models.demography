(ns repl.run-model
  (:require [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [witan.models.load-data :as ld]
            [witan.workspace-api.protocols :as p]
            [schema.core :as s]
            [witan.workspace-executor.core :as wex]
            [witan.models.dem.ccm.models :refer [cohort-component-model
                                                 model-library]]))

(defn write-data-to-csv [data filepath]
  (with-open [out-file (io/writer filepath)]
    (data-csv/write-csv out-file data)))

(defn with-gss
  [id gss-code]
  (str id "_" gss-code ".csv"))

(defn local-download
  [local-inputs input _ schema]
  (let [[key src] (get local-inputs (or (:witan/fn input)
                                        (:witan/name input)))
        r (ld/load-dataset key src)]
    (get r key)))

(defn fix-input
  [local-inputs input]
  (assoc-in input [:witan/params :fn] (partial local-download local-inputs input)))

(defn run-demography-model
  ""
  [gss-code
   first-proj-year last-proj-year
   start-year-avg-domin-mig end-year-avg-domin-mig start-year-avg-domout-mig end-year-avg-domout-mig
   start-year-avg-intin-mig end-year-avg-intin-mig start-year-avg-intout-mig end-year-avg-intout-mig
   proj-asfr-variant future-fert-scenario
   start-year-avg-mort end-year-avg-mort proj-asmr-variant  mort-scenario
   proportion-male-newborns]
  (let [local-inputs {:ccm-core-input/historic-population
                      [:historic-population
                       (with-gss "./datasets/default_datasets/core/historic_population" gss-code)]
                      :ccm-core-input/historic-births
                      [:historic-births
                       (with-gss "./datasets/default_datasets/fertility/historic_births" gss-code)]
                      :ccm-core-input/future-mortality-trend-assumption
                      [:future-mortality-trend-assumption
                       "./datasets/default_datasets/mortality/future_mortality_trend_assumption.csv"]
                      :ccm-core-input/future-fertility-trend-assumption
                      [:future-fertility-trend-assumption
                       "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"]
                      :ccm-core-input/historic-births-by-age-mother
                      [:historic-births-by-age-mother
                       (with-gss "./datasets/default_datasets/fertility/historic_births_by_age_of_mother" gss-code)]
                      :ccm-core-input/historic-deaths
                      [:historic-deaths
                       (with-gss "./datasets/default_datasets/mortality/historic_deaths" gss-code)]
                      :ccm-core-input/domestic-in-migrants
                      [:domestic-in-migrants
                       (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_in" gss-code)]
                      :ccm-core-input/domestic-out-migrants
                      [:domestic-out-migrants
                       (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_out" gss-code)]
                      :ccm-core-input/international-in-migrants
                      [:international-in-migrants
                       (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_in" gss-code)]
                      :ccm-core-input/international-out-migrants
                      [:international-out-migrants
                       (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_out" gss-code)]}
        fixed-catalog (mapv #(-> (if (= (:witan/type %) :input)
                                   (fix-input local-inputs %)
                                   %)
                                 (assoc-in [:witan/params :last-proj-year] last-proj-year)
                                 (assoc-in [:witan/params :first-proj-year] first-proj-year))
                            (:catalog cohort-component-model))
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   fixed-catalog
                       :contracts (p/available-fns (model-library))}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})]
    (first result)))
