(ns witan.models.acceptance.workspace-test
  (:require [clojure.test :refer :all]
            [clojure.set]
            [witan.workspace-api :refer :all]
            [witan.workspace-api.protocols :as p]
            [witan.workspace-executor.core :as wex]
            [witan.models.load-data :as ld]
            [schema.core :as s]
            ;;
            [witan.models.dem.ccm.fert.fertility]
            [witan.models.dem.ccm.mort.mortality]
            [witan.models.dem.ccm.mig.migration]
            [witan.models.dem.ccm.core.projection-loop]
            [witan.models.dem.ccm.models :refer [cohort-component-model
                                                 model-library]]
            [witan.models.dem.ccm.models-utils :refer [make-catalog make-contracts]]
            [witan.datasets :as wds]))

(def gss-code "E06000023")

(defn with-gss
  [id]
  (str id "_" gss-code ".csv"))

(def local-inputs
  {:ccm-core-input/in-hist-popn
   [:historic-population
    (with-gss "./datasets/default_datasets/core/historic_population")]

   :ccm-core-input/in-hist-total-births
   [:historic-births
    (with-gss "./datasets/default_datasets/fertility/historic_births")]

   :ccm-core-input/in-future-mort-trend
   [:future-mortality-trend-assumption
    "./datasets/default_datasets/mortality/future_mortality_trend_assumption.csv"]

   :ccm-core-input/in-future-fert-trend
   [:future-fertility-trend-assumption
    "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"]

   :ccm-core-input/in-proj-births-by-age-of-mother
   [:historic-births-by-age-mother
    (with-gss "./datasets/default_datasets/fertility/historic_births_by_age_of_mother")]

   :ccm-core-input/in-hist-deaths-by-age-and-sex
   [:historic-deaths
    (with-gss "./datasets/default_datasets/mortality/historic_deaths")]

   :ccm-core-input/in-hist-dom-in-migrants
   [:domestic-in-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_in")]

   :ccm-core-input/in-hist-dom-out-migrants
   [:domestic-out-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_out")]

   :ccm-core-input/in-hist-intl-in-migrants
   [:international-in-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_in")]

   :ccm-core-input/in-hist-intl-out-migrants
   [:international-out-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_out")]})

(defn local-download
  [input _ schema]
  (let [[key src] (get local-inputs (or (:witan/fn input)
                                        (:witan/name input)))
        r (ld/load-dataset key src)]
    (get r key)))

(defn fix-input
  [input]
  (assoc-in input [:witan/params :fn] (partial local-download input)))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(deftest workspace-test
  (let [fixed-catalog (mapv #(if (= (:witan/type %) :input) (fix-input %) %) (:catalog cohort-component-model))
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   fixed-catalog
                       :contracts (p/available-fns (model-library))}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})
        test-value    (-> result
                          first
                          :population
                          (wds/select-from-ds {:age 21})
                          (wds/select-from-ds {:year 2015})
                          (wds/select-from-ds {:sex "M"})
                          :columns
                          last
                          (get 0))]
    (is (not-empty result))
    (is (= 4 (count (first result))))
    (is (contains? (first result) :population))
    (is (fp-equals? 4645.734788600881 test-value 0.000001))))

(deftest imodellibrary-test
  (let [ml (model-library)
        models (p/available-models ml)
        fns (p/available-fns ml)]
    (is (not-empty models))
    (is (= (:metadata (first models)) (-> #'cohort-component-model meta :witan/metadata)))
    (is (= (select-keys (first models) [:workflow :catalog]) cohort-component-model))
    (is (not-empty fns))))
