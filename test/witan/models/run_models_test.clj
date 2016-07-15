(ns witan.models.run-models-test
  (:require [witan.models.run-models :refer :all]
            [clojure.test :refer :all]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.datasets :as wds]
            [witan.models.dem.ccm.core.projection-loop :as core]
            [witan.models.dem.ccm.core.projection-loop-test :as core-test]
            [witan.models.dem.ccm.fert.fertility-mvp :as fert]
            [witan.models.dem.ccm.mort.mortality-mvp :as mort]
            [witan.models.dem.ccm.mig.net-migration :as mig]
            [witan.models.dem.ccm.components-functions :as cf]))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;; Load testing data
(def datasets  {:ons-proj-births-by-age-mother
                "resources/default_datasets/fertility/proj-births-by-age-mother.csv"
                :historic-births
                "resources/default_datasets/fertility/historic_births.csv"
                :historic-population
                "resources/default_datasets/core/historic-population.csv"
                :historic-deaths
                "resources/default_datasets/mortality/historic_deaths.csv"
                :domestic-in-migrants
                "resources/default_datasets/migration/domestic_in_migrants.csv"
                :domestic-out-migrants
                "resources/default_datasets/migration/domestic_out_migrants.csv"
                :international-in-migrants
                "resources/default_datasets/migration/international_in_migrants.csv"
                :international-out-migrants
                "resources/default_datasets/migration/international_out_migrants.csv"})

(def gss-bristol "E06000023")

(def params-2015 core-test/params)

(def params-2040 core-test/params-2040)

(deftest get-dataset-test
  (testing "The data is filtered on local authority."
    (let [dataset (:ons-proj-births-by-age-mother
                   (get-dataset
                    :ons-proj-births-by-age-mother
                    "resources/default_datasets/fertility/proj-births-by-age-mother.csv"
                    "E06000023"))
          gss-dataset (i/$ :gss-code dataset)]
      (is (true?
           (every? #(= % "E06000023") gss-dataset))))))

;; Comparing each step happening in run_models and projection_loop
;; namespaces to make sure run_models functions don't induce an error.

(def prepared-inputs-bristol
  (-> datasets
      (get-datasets "E06000023")
      core/prepare-inputs))

(def prepared-inputs-core
  (-> core-test/data-inputs
      core/prepare-inputs))

(deftest prepare-inputs-test
  (testing "The run_model ns doesn't induce any change in the input data"
    (is (= (:shape (:historic-population prepared-inputs-bristol))
           (:shape (:historic-population prepared-inputs-core))))
    (is (= (:shape (:historic-births prepared-inputs-bristol))
           (:shape (:historic-births prepared-inputs-core))))
    (is (= (:shape (:historic-deaths prepared-inputs-bristol))
           (:shape (:historic-deaths prepared-inputs-core))))
    (let [joined-popn (wds/join (:historic-population prepared-inputs-bristol)
                                (ds/rename-columns (:historic-population prepared-inputs-core)
                                                   {:popn :popn-core})
                                [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-popn :rows % :cols :popn-core)
                               (i/sel joined-popn :rows % :cols :popn) 0.0001)
                  (range (first (:shape joined-popn))))))
    (let [joined-births (wds/join (:historic-births prepared-inputs-bristol)
                                  (ds/rename-columns (:historic-births prepared-inputs-core)
                                                     {:births :births-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-births :rows % :cols :births-core)
                               (i/sel joined-births :rows % :cols :births) 0.0001)
                  (range (first (:shape joined-births))))))
    (let [joined-deaths (wds/join (:historic-deaths prepared-inputs-bristol)
                                  (ds/rename-columns (:historic-deaths prepared-inputs-core)
                                                     {:deaths :deaths-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-deaths :rows % :cols :deaths-core)
                               (i/sel joined-deaths :rows % :cols :deaths) 0.0001)
                  (range (first (:shape joined-deaths))))))))

(def prep-fert-bristol (fert/fertility-pre-projection prepared-inputs-bristol params-2015))

(def prep-fert-core (fert/fertility-pre-projection prepared-inputs-core params-2015))

(deftest prepare-fertility-test
  (testing "The run_model ns doesn't induce any change in the fertility preparation"
    (is (= (:shape (:historic-population prep-fert-bristol))
           (:shape (:historic-population prep-fert-core))))
    (is (= (:shape (:historic-births prep-fert-bristol))
           (:shape (:historic-births prep-fert-core))))
    (is (= (:shape (:historic-deaths prep-fert-bristol))
           (:shape (:historic-deaths prep-fert-core))))
    (let [joined-popn (wds/join (:historic-population prep-fert-bristol)
                                (ds/rename-columns (:historic-population prep-fert-core)
                                                   {:popn :popn-core})
                                [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-popn :rows % :cols :popn-core)
                               (i/sel joined-popn :rows % :cols :popn) 0.0001)
                  (range (first (:shape joined-popn))))))
    (let [joined-births (wds/join (:historic-births prep-fert-bristol)
                                  (ds/rename-columns (:historic-births prep-fert-core)
                                                     {:births :births-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-births :rows % :cols :births-core)
                               (i/sel joined-births :rows % :cols :births) 0.0001)
                  (range (first (:shape joined-births))))))
    (let [joined-deaths (wds/join (:historic-deaths prep-fert-bristol)
                                  (ds/rename-columns (:historic-deaths prep-fert-core)
                                                     {:deaths :deaths-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-deaths :rows % :cols :deaths-core)
                               (i/sel joined-deaths :rows % :cols :deaths) 0.0001)
                  (range (first (:shape joined-deaths))))))))

(def popn-at-risk-core (mort/create-popn-at-risk-death
                        (:historic-population prep-fert-core)
                        (:historic-deaths prep-fert-core)
                        (:historic-births prep-fert-core)))

(def popn-at-risk-bristol (mort/create-popn-at-risk-death
                           (:historic-population prep-fert-bristol)
                           (:historic-deaths prep-fert-bristol)
                           (:historic-births prep-fert-bristol)))

(deftest create-popn-at-risk-death-test
  (testing "The run_model ns doesn't induce any change in the mortality popn at risk"
    (is (= (:shape popn-at-risk-bristol)
           (:shape popn-at-risk-core)))
    (let [joined-popn-at-risk (wds/join popn-at-risk-bristol
                                        (ds/rename-columns popn-at-risk-core
                                                           {:popn :popn-core})
                                        [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-popn-at-risk :rows % :cols :popn-core)
                               (i/sel joined-popn-at-risk :rows % :cols :popn) 0.0001)
                  (range (first (:shape joined-popn-at-risk))))))))

(def calc-hist-asmr-core (mort/calc-death-rates (:historic-deaths prep-fert-core)
                                           popn-at-risk-core))

(def calc-hist-asmr-bristol (mort/calc-death-rates (:historic-deaths prep-fert-bristol)
                                              popn-at-risk-bristol))

(deftest calc-death-rates-test
  (testing "The run_model ns doesn't induce any change in the hist asmr calc"
    (is (= (:shape calc-hist-asmr-bristol)
           (:shape calc-hist-asmr-core)))
    (let [joined-hist-asmr (wds/join calc-hist-asmr-bristol
                                     (ds/rename-columns calc-hist-asmr-core
                                                        {:death-rate :death-rate-core})
                                     [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-hist-asmr :rows % :cols :death-rate-core)
                               (i/sel joined-hist-asmr :rows % :cols :death-rate) 0.0001)
                  (range (first (:shape joined-hist-asmr))))))))

(deftest project-method-avg-test
  (testing "The run_model ns doesn't induce any change in the hist asmr proj avg calc"
    (let [avg-proj-bristol (cf/jumpoffyr-method-average calc-hist-asmr-bristol
                                                        :death-rate
                                                        :death-rate
                                                        (:start-yr-avg-mort params-2015)
                                                        (:end-yr-avg-mort params-2015))
          avg-proj-core (cf/jumpoffyr-method-average calc-hist-asmr-core
                                                     :death-rate
                                                     :death-rate
                                                     (:start-yr-avg-mort params-2015)
                                                     (:end-yr-avg-mort params-2015))
          joined-avg-proj (wds/join avg-proj-bristol
                                    (ds/rename-columns avg-proj-core
                                                       {:death-rate :death-rate-core})
                                    [:gss-code :sex :age])]
      (is (every? #(fp-equals? (i/sel joined-avg-proj :rows % :cols :death-rate-core)
                               (i/sel joined-avg-proj :rows % :cols :death-rate) 0.0001)
                  (range (first (:shape joined-avg-proj))))))))

(def hist-asmr-bristol (mort/calc-historic-asmr prep-fert-bristol))

(def hist-asmr-core (mort/calc-historic-asmr prep-fert-core))

(deftest calc-asmr-test
  (testing "The run_model ns doesn't induce any change in the hist asmr projection"
    (let [calc-asmr-bristol (:historic-asmr hist-asmr-bristol)
          calc-asmr-core (ds/rename-columns
                             (:historic-asmr hist-asmr-core)
                             {:death-rate :death-rate-core})
          joined-calc-asmr (wds/join calc-asmr-bristol
                                        calc-asmr-core
                                        [:gss-code :sex :age :year])]
      (is (= (:shape calc-asmr-bristol)
             (:shape calc-asmr-core)))
      (is (every? #(fp-equals? (i/sel joined-calc-asmr :rows % :cols :death-rate-core)
                               (i/sel joined-calc-asmr :rows % :cols :death-rate) 0.0001)
                  (range (first (:shape joined-calc-asmr))))))))

(def project-asmr-bristol (mort/project-asmr hist-asmr-bristol params-2015))

(def project-asmr-core (mort/project-asmr hist-asmr-core params-2015))

(deftest project-asmr-test
  (testing "The run_model ns doesn't induce any change in the hist asmr projection"
    (let [proj-asmr-bristol (:initial-projected-mortality-rates project-asmr-bristol)
          proj-asmr-core (ds/rename-columns
                          (:initial-projected-mortality-rates project-asmr-core)
                          {:death-rate :death-rate-core})
          joined-proj-asmr (wds/join proj-asmr-bristol
                                     proj-asmr-core
                                     [:gss-code :sex :age])]
      (is (= (:shape project-asmr-bristol)
             (:shape project-asmr-core)))
      (is (every? #(fp-equals? (i/sel joined-proj-asmr :rows % :cols :death-rate-core)
                               (i/sel joined-proj-asmr :rows % :cols :death-rate) 0.0001)
                  (range (first (:shape joined-proj-asmr))))))))

(def prep-mort-bristol (mort/mortality-pre-projection prep-fert-bristol params-2015))

(def prep-mort-core (mort/mortality-pre-projection prep-fert-core params-2015))

(deftest prepare-mortality-test
  (testing "The run_model ns doesn't induce any change in the mortality preparation"
    (is (= (:shape (:historic-population prep-mort-bristol))
           (:shape (:historic-population prep-mort-core))))
    (is (= (:shape (:historic-births prep-mort-bristol))
           (:shape (:historic-births prep-mort-core))))
    (is (= (:shape (:historic-deaths prep-mort-bristol))
           (:shape (:historic-deaths prep-mort-core))))
    (let [joined-popn (wds/join (:historic-population prep-mort-bristol)
                                (ds/rename-columns (:historic-population prep-mort-core)
                                                   {:popn :popn-core})
                                [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-popn :rows % :cols :popn-core)
                               (i/sel joined-popn :rows % :cols :popn) 0.0001)
                  (range (first (:shape joined-popn))))))
    (let [joined-births (wds/join (:historic-births prep-mort-bristol)
                                  (ds/rename-columns (:historic-births prep-mort-core)
                                                     {:births :births-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-births :rows % :cols :births-core)
                               (i/sel joined-births :rows % :cols :births) 0.0001)
                  (range (first (:shape joined-births))))))
    (let [joined-deaths (wds/join (:historic-deaths prep-mort-bristol)
                                  (ds/rename-columns (:historic-deaths prep-mort-core)
                                                     {:deaths :deaths-core})
                                  [:gss-code :sex :age :year])]
      (is (every? #(fp-equals? (i/sel joined-deaths :rows % :cols :deaths-core)
                               (i/sel joined-deaths :rows % :cols :deaths) 0.0001)
                  (range (first (:shape joined-deaths))))))))

(deftest run-ccm-test
  (testing "The historical and projection data is returned"
    (let [proj-core-2015 (ds/rename-columns (i/query-dataset
                                             (core/looping-test core-test/data-inputs params-2015)
                                             {:year {:$eq 2015}})
                                            {:popn :popn-core})
          proj-bristol-2015 (i/query-dataset (run-ccm datasets gss-bristol params-2015)
                                             {:year {:$eq 2015}})
          proj-core-2040 (ds/rename-columns (i/query-dataset
                                             (core/looping-test core-test/data-inputs params-2040)
                                             {:year {:$eq 2040}})
                                            {:popn :popn-core})
          proj-bristol-2040 (i/query-dataset (run-ccm datasets gss-bristol params-2040)
                                             {:year {:$eq 2040}})
          joined-proj-2015 (wds/join proj-bristol-2015 proj-core-2015 [:gss-code :sex :age :year])
          joined-proj-2040 (wds/join proj-bristol-2040 proj-core-2040 [:gss-code :sex :age :year])]
      ;; Compare projection values for 2015:
      (is (every? #(fp-equals? (i/sel joined-proj-2015 :rows % :cols :popn)
                               (i/sel joined-proj-2015 :rows % :cols :popn-core) 0.0001)
                  (range (first (:shape joined-proj-2015)))))
      ;; Compare projection values for 2040:
      (is (every? #(fp-equals? (i/sel joined-proj-2040 :rows % :cols :popn)
                               (i/sel joined-proj-2040 :rows % :cols :popn-core) 0.0001)
                  (range (first (:shape joined-proj-2040))))))))


(deftest get-district-test
  (testing "fn recovers correct district name"
    (is (= (get-district "E06000023") "Bristol, City of"))))

(def input-data-set (ds/dataset [{:year 2014 :gss-code "E06000023"}
                                 {:year 2015 :gss-code "E06000023"}]))

(def output-data-set (ds/dataset [{:year 2014 :gss-code "E06000023" :district-out "Bristol, City of"}
                                  {:year 2015 :gss-code "E06000023" :district-out "Bristol, City of"}]))

(deftest add-district-to-dataset-per-user-input-test
  (testing "district column added to dataset and populated"
    (let [district-added (add-district-to-dataset-per-user-input input-data-set "E06000023")
          joined-data (wds/join district-added output-data-set [:gss-code :year])]
      (is (every? #(= (i/sel joined-data :rows % :cols :district)
                      (i/sel joined-data :rows % :cols :district-out))
                  (range (first (:shape joined-data))))))))
