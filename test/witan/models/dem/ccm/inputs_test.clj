(ns witan.models.dem.ccm.inputs-test
  (:require [witan.models.dem.ccm.inputs :as sut]
            [witan.models.acceptance.workspace-test :as wt]
            [clojure.test :refer :all]))

(deftest inputs
  (is (contains? (sut/in-hist-deaths-by-age-and-sex-1-0-0
                  nil {:src "./datasets/test_datasets/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                       :fn (partial wt/local-download (:witan/metadata (meta #'sut/in-hist-deaths-by-age-and-sex-1-0-0)))})
                 :historic-deaths)))
