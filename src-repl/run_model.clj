(ns repl.run-model
  (:require [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [witan.models.load-data :as ld]
            [witan.workspace-api.protocols :as p]
            [schema.core :as s]
            [witan.workspace-executor.core :as wex]
            [clojure.core.matrix.dataset :as ds]
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

(defn convert-row-maps-to-vector-of-vectors
  "Takes a sequence of maps and returns a vector of vectors where the first vector
   contains column names and the following vetors contain the value for each
   row. This is the format needed to save as csv using the write-to-csv function"
  [rows-as-maps]
  (let [colnames (mapv name (keys (first rows-as-maps)))
        rows (mapv #(vec (vals %)) rows-as-maps)]
    (into [colnames] rows)))

(defn to-csv [data filepath]
  (with-open [out-file (io/writer filepath)]
    (data-csv/write-csv out-file data)))

(defn run-demography-model
  ""
  [gss-code
   in-migrant-multiplier
   first-proj-year last-proj-year]
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
        catalog (mapv #(-> (if (= (:witan/type %) :input)
                             (fix-input local-inputs %)
                             %)
                           (assoc-in [:witan/params :first-proj-year] first-proj-year)
                           (assoc-in [:witan/params :last-proj-year] last-proj-year)
                           (assoc-in [:witan/params :in-migrant-multiplier] in-migrant-multiplier))
                      (:catalog cohort-component-model))
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   catalog
                       :contracts (p/available-fns (model-library))}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})
        as-rows       (map ds/row-maps (vals (first result)))
        output        (hash-map :births (convert-row-maps-to-vector-of-vectors (nth as-rows 0))
                                :population (convert-row-maps-to-vector-of-vectors (nth as-rows 1))
                                :net-migration (convert-row-maps-to-vector-of-vectors (nth as-rows 2))
                                :mortality (convert-row-maps-to-vector-of-vectors (nth as-rows 3)))]
    (to-csv (:births output) (str "./output/" (name :births) ".csv"))
    (to-csv (:population  output) (str "./output/" (name :population) ".csv"))
    (to-csv (:net-migration output) (str "./output/" (name :net-migration) ".csv"))
    (to-csv (:mortality output) (str "./output/" (name :mortality) ".csv"))))
