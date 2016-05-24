(ns witan.models.dem.ccm.components-functions-test
  (:require [witan.models.dem.ccm.components-functions :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))


(def domestic-mig-in (ld/load-dataset :migration-estimates
                      "resources/test_data/bristol_dom_in_mig.csv"))

(deftest calculate-averages-test
  (testing "The fonction return the averages on the right year period"
    ))
