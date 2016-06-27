(ns witan.models.dem.ccm.mig.net-migration-test
  (:require [witan.models.dem.ccm.mig.net-migration :refer :all]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.datasets :as wds]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(def migration-data (ld/load-datasets {:domestic-in-migrants
                                       "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                       :domestic-out-migrants
                                       "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                       :international-in-migrants
                                       "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                       :international-out-migrants
                                       "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"}))

(def net-migration-r (:net-migration (ld/load-dataset
                                      :net-migration
                                      "test_data/r_outputs_for_testing/mig/bristol_migration_module_r_output_2015.csv")))

(def params {:number-of-years 12 :jumpoff-year 2015})

(deftest combine-into-net-flows-test
  (testing "The net migration flows are calculated correctly."
    (let [net-mig-r (ds/rename-columns net-migration-r {:net-mig :net-mig-r})
          joined-mig (-> migration-data
                         (project-domestic-in-migrants params)
                         (project-domestic-out-migrants params)
                         (project-international-in-migrants params)
                         (project-international-out-migrants params)
                         combine-into-net-flows
                         :net-migration
                         (wds/join net-mig-r [:gss-code :sex :age]))]
      (is (every? #(fp-equals? (i/sel joined-mig :rows % :cols :net-mig-r)
                               (i/sel joined-mig :rows % :cols :net-mig)
                               0.0000000001)
                  (range (first (:shape joined-mig))))))))
