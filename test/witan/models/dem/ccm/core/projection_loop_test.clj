(ns witan.models.dem.ccm.core.projection-loop-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.core.projection-loop :refer :all]
            [clojure.test :as t]
            [clojure.string :as str]
            [clojure.core.matrix.dataset :as ds]
            [clojure.core.matrix :as m]
            [incanter.core :as i]
            [witan.models.load-data :as ld]))

;; Load testing data
;; NEED TO GET THE RIGHT INPUT -> mye.est
(def data-inputs (prepare-inputs
                  (ld/load-datasets
                   {:population
                    "resources/test_data/bristol_hist_popn_est.csv"
                    :births
                    "resources/test_data/handmade_outputs/bristol_fertility_module_handmade_output.csv"
                    :deaths
                    "resources/test_data/handmade_outputs/bristol_mortality_module_handmade_output.csv"
                    :net-migration
                    "resources/test_data/handmade_outputs/bristol_migration_module_handmade_output.csv"})))

(def params {:first-proj-year 2014
             :last-proj-year 2015})

(def output-2015 (ld/load-datasets
                  {:end-population
                   "resources/test_data/handmade_outputs/bristol_end_population_2015.csv"}))

;; Useful fns:
(defn- same-coll? [coll1 coll2]
  (= (set coll1) (set coll2)))

(defn- fp-equals? [x y ε] (< (Math/abs (- x y)) ε))

;;get sum of population for particular year & age group in dataset with :popn column
(def sum-popn (fn [popn year age]
                (apply + (i/$ :popn (i/query-dataset popn
                                                     {:year year :age age})))))
(def get-popn (fn
                ([ds popn age sex]
                 (i/$ popn (i/query-dataset ds {:age age :sex sex})))
                ([ds popn year age sex]
                 (i/$ popn (i/query-dataset ds {:year year :age age :sex sex})))))

;; Tests:
(deftest select-starting-popn-test
  (testing "The starting popn is returned"
    (let [get-start-popn (select-starting-popn data-inputs)]
      (is (same-coll? [:gss-code :sex :age :year :popn]
                      (ds/column-names (:latest-yr-popn get-start-popn))))
      (is (true? (every? #(= % 2015) (i/$ :year (:latest-yr-popn get-start-popn)))))
      ;; Total number of 15 yrs old in 2015:
      (is (= 4386.0 (apply + (i/$ :popn
                                  (i/query-dataset (:latest-yr-popn get-start-popn)
                                                   {:year 2015 :age 15})))))
      ;; Total number of women aged between 40 and 43 in 2015:
      (is (= 5394.0 (apply + (i/$ :popn
                                  (i/query-dataset (:latest-yr-popn get-start-popn)
                                                   {:sex "F" :age {:$gt 40 :lt 43}
                                                    :year 2015}))))))))

(deftest age-on-test
  (testing "Popn is aged on correctly, 90 grouping is handled correctly."
    (let [starting-popn (:latest-yr-popn (select-starting-popn data-inputs))
          aged-popn (:latest-yr-popn (age-on (select-starting-popn data-inputs)))]
      (is (= (sum-popn starting-popn 2015 15)
             (sum-popn aged-popn 2015 16)))
      (is (= (+ (sum-popn starting-popn 2015 89) (sum-popn starting-popn 2015 90))
             (sum-popn aged-popn 2015 90)))
      (is (= 2 (first (:shape (i/query-dataset aged-popn {:year 2015 :age 90}))))))))

(deftest add-births-test
  (testing "Newborns are correctly added to projection year popn."
    (let [births-added (add-births (age-on (select-starting-popn data-inputs)))
          popn-with-births (:latest-yr-popn births-added)
          latest-yr (:loop-year births-added)
          latest-newborns (i/query-dataset popn-with-births {:year latest-yr :age 0})]
      (is (= 2 (first (:shape latest-newborns))))
      (is (fp-equals? (+ 3135.891642 3292.686224)
                      (#(apply + (i/$ :popn %)) latest-newborns) 0.0001)))))

(deftest remove-deaths-test
  (testing "The deaths are removed from the popn."
    (let [starting-popn (select-starting-popn data-inputs)
          aged-on-popn (age-on starting-popn)
          popn-with-births (add-births aged-on-popn)
          popn-wo-deaths (remove-deaths popn-with-births)]
      (is (= (- (get-popn (:latest-yr-popn popn-with-births) :popn 2015 0 "F")
                (get-popn (:deaths popn-with-births) :deaths 0 "F"))
             (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F"))))))

(deftest apply-migration-test
  (testing "The migrants are added to the popn."
    (let [starting-popn (select-starting-popn data-inputs)
          aged-on-popn (age-on starting-popn)
          popn-with-births (add-births aged-on-popn)
          popn-wo-deaths (remove-deaths popn-with-births)
          popn-with-mig (apply-migration popn-wo-deaths)]
      (is (= (+ (get-popn (:latest-yr-popn popn-wo-deaths) :popn 2015 0 "F")
                (get-popn (:net-migration popn-wo-deaths) :net-mig 0 "F"))
             (get-popn (:latest-yr-popn popn-with-mig) :popn 2015 0 "F"))))))

(deftest looping-test-test
  (testing "The output of the loop matches the R code."
    (let [proj-clj (looping-test data-inputs {:first-proj-year 2014
                                              :last-proj-year 2015})
          proj-clj-2015 (ds/rename-columns
                         (i/query-dataset proj-clj
                                          {:year
                                           {:$eq 2015}})
                         {:popn :popn-clj})
          proj-r-2015 (ds/rename-columns
                       (:end-population output-2015) {:popn :popn-r})
          r-clj-2015 (i/$join [[:gss-code :sex :age :year] [:gss-code :sex :age :year]]
                              proj-r-2015 proj-clj-2015)]
      ;; Compare all values in the :popn column between the R and Clj results for 2015:
      (is (every? #(fp-equals? (i/sel r-clj-2015 :rows % :cols :popn-r)
                               (i/sel r-clj-2015 :rows % :cols :popn-clj) 0.0001)
                  (range (first (:shape r-clj-2015))))))))
