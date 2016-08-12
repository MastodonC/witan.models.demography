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
            [witan.models.dem.ccm.models-utils :refer [make-catalog make-contracts]]))

(def local-inputs
  {:ccm-core-input/in-hist-popn
   [:historic-population
    "./datasets/test_datasets/model_inputs/bristol_hist_popn_mye.csv"]

   :ccm-core-input/in-hist-total-births
   [:historic-births
    "./datasets/test_datasets/model_inputs/fert/bristol_hist_births_mye.csv"]

   :ccm-core-input/in-future-mort-trend
   [:future-mortality-trend-assumption
    "./datasets/test_datasets/model_inputs/mort/death_improvement.csv"]

   :ccm-core-input/in-proj-births-by-age-of-mother
   [:ons-proj-births-by-age-mother
    "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"]

   :ccm-core-input/in-hist-deaths-by-age-and-sex
   [:historic-deaths
    "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"]

   :ccm-core-input/in-hist-dom-in-migrants
   [:domestic-in-migrants
    "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"]

   :ccm-core-input/in-hist-dom-out-migrants
   [:domestic-out-migrants
    "./datasets/test_datasets/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"]

   :ccm-core-input/in-hist-intl-in-migrants
   [:international-in-migrants
    "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_inmigrants.csv"]

   :ccm-core-input/in-hist-intl-out-migrants
   [:international-out-migrants
    "./datasets/test_datasets/model_inputs/mig/bristol_hist_international_outmigrants.csv"]})

(defn local-download
  [input _ schema]
  (let [[key src] (get local-inputs (:witan/fn input))
        r (ld/load-dataset key src)]
    (get r key)))

(defn fix-input
  [input]
  (assoc-in input [:witan/params :fn] (partial local-download input)))

(deftest workspace-test
  (let [fixed-catalog (mapv #(if (= (:witan/type %) :input) (fix-input %) %) (:catalog cohort-component-model))
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   fixed-catalog
                       :contracts (p/available-fns (model-library))}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})]
    (is (not-empty result))
    (is (= 1 (count (first result))))
    (is (contains? (first result) :population))))

(deftest imodellibrary-test
  (let [ml (model-library)
        models (p/available-models ml)
        fns (p/available-fns ml)]
    (is (not-empty models))
    (is (= (:metadata (first models)) (-> #'cohort-component-model meta :witan/metadata)))
    (is (= (select-keys (first models) [:workflow :catalog]) cohort-component-model))
    (is (not-empty fns))))
