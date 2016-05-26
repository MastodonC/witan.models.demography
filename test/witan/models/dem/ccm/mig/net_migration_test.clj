(ns witan.models.dem.ccm.mig.net-migration-test
  (:require [witan.models.dem.ccm.mig.net-migration :refer :all]
            [witan.models.dem.ccm.components-functions :as cf]
            [witan.models.load-data :as ld]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [clojure.set :as set]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

(def migration-data (ld/load-datasets {:domestic-in-migrants
                                       "resources/test_data/migration/bristol_dom_in_mig.csv"
                                        :domestic-out-migrants
                                       "resources/test_data/migration/bristol_dom_out_mig.csv"
                                        :international-in-migrants
                                       "resources/test_data/migration/bristol_inter_in_mig.csv"
                                        :international-out-migrants
                                       "resources/test_data/migration/bristol_inter_in_mig.csv"}))

(def net-migration-r (:net-migration (ld/load-dataset
                                      :net-migration
                                      "resources/test_data/handmade_outputs/bristol_migration_module_handmade_output.csv")))

(def params {:number-of-years 12 :jumpoff-year 2015})

(deftest combine-into-net-flows-test
  (testing "The net migration flows are calculated correctly."
    (let [net-migration-clj (cf/order-ds (-> migration-data
                                             (project-domestic-in-migrants params)
                                             (project-domestic-out-migrants params)
                                             (project-international-in-migrants params)
                                             (project-international-out-migrants params)
                                             combine-into-net-flows
                                             :net-migration)
                                         [:sex :age])
          net-mig-r (ds/rename-columns net-migration-r {:net-mig :net-mig-r})
          joined-mig (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                              net-migration-clj net-mig-r)]
      (is (every? #(fp-equals? (i/sel joined-mig :rows % :cols :net-mig-r)
                              (i/sel joined-mig :rows % :cols :net-mig)
                              0.0001)
                  (range (first (:shape joined-mig))))))))
