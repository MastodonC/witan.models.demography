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
            [witan.datasets :as wds]
            [clojure.core.matrix.dataset :as ds]))

(def gss-code "E06000023")

(defn with-gss
  [id]
  (str id "_" gss-code ".csv"))

(def local-inputs
  {:ccm-core-input/historic-population
   [:historic-population
    (with-gss "./datasets/default_datasets/core/historic_population")]

   :ccm-core-input/historic-births
   [:historic-births
    (with-gss "./datasets/default_datasets/fertility/historic_births")]

   :ccm-core-input/future-mortality-trend-assumption
   [:future-mortality-trend-assumption
    "./datasets/default_datasets/mortality/future_mortality_trend_assumption.csv"]

   :ccm-core-input/future-fertility-trend-assumption
   [:future-fertility-trend-assumption
    "./datasets/default_datasets/fertility/future_fertility_trend_assumption.csv"]

   :ccm-core-input/historic-births-by-age-mother
   [:historic-births-by-age-mother
    (with-gss "./datasets/default_datasets/fertility/historic_births_by_age_of_mother")]

   :ccm-core-input/historic-deaths
   [:historic-deaths
    (with-gss "./datasets/default_datasets/mortality/historic_deaths")]

   :ccm-core-input/domestic-in-migrants
   [:domestic-in-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_in")]

   :ccm-core-input/domestic-out-migrants
   [:domestic-out-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_domestic_out")]

   :ccm-core-input/international-in-migrants
   [:international-in-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_in")]

   :ccm-core-input/international-out-migrants
   [:international-out-migrants
    (with-gss "./datasets/default_datasets/migration/historic_migration_flows_international_out")]})

(def r-output-2015 (ld/load-datasets
                    {:end-population
                     "./datasets/test_datasets/r_outputs_for_testing/core/bristol_end_population_2015.csv"}))

(def r-output-2040 (ld/load-datasets
                    {:end-population
                     "./datasets/test_datasets/r_outputs_for_testing/core/bristol_end_population_2040.csv"}))

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
  (let [fixed-catalog (mapv #(if (= (:witan/type %) :input)
                               (fix-input %)
                               (if (get-in % [:witan/params :last-proj-year])
                                 (assoc-in % [:witan/params :last-proj-year] 2040)
                                 %))
                            (:catalog cohort-component-model))
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   fixed-catalog
                       :contracts (p/available-fns (model-library))}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})
        popn-data     (:population (first result))
        popn-data-2015 (wds/select-from-ds popn-data {:year 2015})
        joined-data-2015 (wds/join (ds/rename-columns (:end-population r-output-2015) {:popn :popn-r})
                                   popn-data-2015
                                   [:gss-code :sex :age :year])
        popn-data-2040 (wds/select-from-ds popn-data {:year 2040})
        joined-data-2040 (wds/join (ds/rename-columns (:end-population r-output-2040) {:popn :popn-r})
                                   popn-data-2040
                                   [:gss-code :sex :age :year])]
    (is (not-empty result))
    (is (= 4 (count (first result))))
    (is (contains? (first result) :population))
    (is (every? #(fp-equals? (wds/subset-ds joined-data-2015 :rows % :cols :popn)
                             (wds/subset-ds joined-data-2015 :rows % :cols :popn-r) 0.0000000001)
                (range (first (:shape joined-data-2015)))))
    (is (every? #(fp-equals? (wds/subset-ds joined-data-2040 :rows % :cols :popn)
                             (wds/subset-ds joined-data-2040 :rows % :cols :popn-r) 0.001)
                (range (first (:shape joined-data-2040)))))
    (let [test-value (-> result
                         first
                         :population
                         (wds/select-from-ds {:age 21})
                         (wds/select-from-ds {:year 2015})
                         (wds/select-from-ds {:sex "M"})
                         :columns
                         last
                         (get 0))]
      (is (fp-equals? 4645.734788600881 test-value 0.000001)))))

(deftest imodellibrary-test
  (let [ml (model-library)
        models (p/available-models ml)
        fns (p/available-fns ml)]
    (is (not-empty models))
    (is (= (:metadata (first models)) (-> #'cohort-component-model meta :witan/metadata)))
    (is (= (select-keys (first models) [:workflow :catalog]) cohort-component-model))
    (is (not-empty fns))))
