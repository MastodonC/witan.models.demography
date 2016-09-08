(ns witan.models.dem.ccm.inputs-test
  (:require [witan.models.dem.ccm.inputs :as sut]
            [witan.models.acceptance.workspace-test :as wt]
            [clojure.test :refer :all]))

(deftest inputs
  (let [r (sut/in-hist-deaths-by-age-and-sex-1-0-0
           nil {:src "./datasets/test_datasets/model_inputs/mort/bristol_hist_deaths_mye.csv"
                :fn (partial wt/local-download (:witan/metadata (meta #'sut/in-hist-deaths-by-age-and-sex-1-0-0)))})]
    (is (contains? r :historic-deaths) "If this test fails, you probably need to run lein split-data first.")))
