(ns witan.models.dem.ccm.mig.net-migration-test
  (:require [witan.models.dem.ccm.mig.net-migration :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [clojure.set :as set]))

(def migration-data (ld/load-datasets {:domestic-in-migrants
                                        "resources/test_data/bristol_dom_in_mig.csv"
                                        :domestic-out-migrants
                                       "resources/test_data/bristol_dom_out_mig.csv"
                                        :international-in-migrants
                                        "resources/test_data/bristol_inter_in_mig.csv"
                                        :international-out-migrants
                                        "resources/test_data/bristol_inter_in_mig.csv"}))

(def params {:number-of-years 12 :jumpoff-year 2015})

(deftest combine-into-net-flows-test
  (testing "The net migration flows are calculated correctly."
    (let [net-migration (-> migration-data
                            (project-domestic-in-migrants params)
                            (project-domestic-out-migrants params)
                            (project-international-in-migrants params)
                            (project-international-out-migrants params)
                            combine-into-net-flows
                            :net-migration)]
      (is (true? true))))) ;; Waiting for a test here
