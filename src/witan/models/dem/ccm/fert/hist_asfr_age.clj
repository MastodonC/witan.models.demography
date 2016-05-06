(ns witan.models.dem.ccm.fert.hist-asfr-age
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn]]))

;; Input schemas:
(def BirthsDataSchema {:column-names [(s/one (s/eq :gss-code) ":gss-code")
                                      (s/one (s/eq :sex) ":sex")
                                      (s/one (s/eq :age) ":age")
                                      (s/one (s/eq :births) ":births")
                                      (s/one (s/eq :year) ":year")]
                       :columns [(s/one [s/Str] "col gss-code")
                                 (s/one [s/Str] "col sex")
                                 (s/one [s/Int] "col age")
                                 (s/one [s/Num] "col births")
                                 (s/one [s/Int] "col year")]
                       s/Keyword s/Any})

(def AtRiskPopnSchema {:column-names [(s/one (s/eq :gss-code) ":gss-code")
                                      (s/one (s/eq :sex) ":sex")
                                      (s/one (s/eq :age) ":age")
                                      (s/one (s/eq :year) ":year")
                                      (s/one (s/eq :popn) ":popn")
                                      (s/one (s/eq :actualyear) ":actualyear")
                                      (s/one (s/eq :actualage) ":actualage")]
                       :columns [(s/one [s/Str] "col gss-code")
                                 (s/one [s/Str] "col sex")
                                 (s/one [s/Int] "col age")
                                 (s/one [s/Int] "col year")
                                 (s/one [s/Num] "col popn")
                                 (s/one [s/Int] "col actualyear")
                                 (s/one [s/Int] "col actualage")]
                       s/Keyword s/Any})

(def AtRiskThisYearSchema {:column-names [(s/one (s/eq :gss-code) ":gss-code")
                                          (s/one (s/eq :sex) ":sex")
                                          (s/one (s/eq :popn-this-yr) ":popn-this-yr")
                                          (s/one (s/eq :age) ":age")]
                           :columns [(s/one [s/Str] "col gss-code")
                                     (s/one [s/Str] "col sex")
                                     (s/one [s/Num] "col popn-this-yr")
                                     (s/one [s/Int] "col age")]
                           s/Keyword s/Any})

(def AtRiskLastYearSchema {:column-names [(s/one (s/eq :gss-code) ":gss-code")
                                          (s/one (s/eq :sex) ":sex")
                                          (s/one (s/eq :age) ":age")
                                          (s/one (s/eq :year) ":year")
                                          (s/one (s/eq :popn-last-yr) ":popn-last-yr")]
                           :columns [(s/one [s/Str] "col gss-code")
                                     (s/one [s/Str] "col sex")
                                     (s/one [s/Int] "col age")
                                     (s/one [s/Int] "col year")
                                     (s/one [s/Num] "col popn-last-yr")]
                           s/Keyword s/Any})

(def BirthsPoolSchema {:column-names [(s/one (s/eq :age) ":age")
                                      (s/one (s/eq :sex) ":sex")
                                      (s/one (s/eq :year) ":year")
                                      (s/one (s/eq :gss-code) ":gss-code")
                                      (s/one (s/eq :birth-pool) ":birth-pool")]
                       :columns [(s/one [s/Int] "col age")
                                 (s/one [s/Str] "col sex")
                                 (s/one [(s/maybe s/Int)] "col year") ;; Bypass missing values `nil`
                                 (s/one [s/Str] "col gss-code")
                                 (s/one [s/Num] "col birth-pool")]
                       s/Keyword s/Any})


;; "Takes births-data dataset
;;    Returns maximum value in Year column of births-data"

(defworkflowfn ->births-data-year
  {:witan/name :historic-births-data-yr
   :witan/version "1.0"
   :witan/input-schema {:births-data BirthsDataSchema}
   :witan/output-schema {:yr s/Int}}
  [{:keys [births-data]} _]
  {:yr (->> births-data
            (i/$ :year)
            (reduce max))})


  ;; "Filters ds for actualyear = yr, removes three columns
  ;; and rename two columns."

(defworkflowfn ->at-risk-this-year
  {:witan/name :popn-at-risk-this-yr
   :witan/version "1.0"
   :witan/input-schema {:at-risk-popn AtRiskPopnSchema
                        :yr s/Int}
   :witan/output-schema {:at-risk-this-year AtRiskThisYearSchema}}
  [{:keys [at-risk-popn yr]} _]
  {:at-risk-this-year (-> at-risk-popn
                          (i/query-dataset {:actualyear yr})
                          (ds/remove-columns [:age])
                          (ds/rename-columns {:actualage :age :popn :popn-this-yr})
                          (ds/remove-columns [:year :actualyear]))})

  ;; "Filters ds for actualyear = yr - 1, removes three columns
  ;; and rename one column."

(defworkflowfn ->at-risk-last-year
  {:witan/name :popn-at-risk-last-yr
   :witan/version "1.0"
   :witan/input-schema {:at-risk-popn AtRiskPopnSchema
                        :yr s/Int}
   :witan/output-schema {:at-risk-last-year AtRiskLastYearSchema}}
  [{:keys [at-risk-popn yr]} _]
  {:at-risk-last-year (-> at-risk-popn
                          (i/query-dataset {:actualyear (dec yr)})
                          (ds/rename-columns {:popn :popn-last-yr})
                          (ds/remove-columns [:actualage :actualyear]))})

;; "Calculates birth pool as avg of at risk popn in births-data's max year & max year - 1
;;    Inputs:  at-risk-this-year ds and at-risk-last year ds
;;    Outputs: dataset with cols gss-code, sex, age, year, birth-pool"

(defworkflowfn ->births-pool
  {:witan/name :births-pool
   :witan/version "1.0"
   :witan/input-schema {:at-risk-this-year AtRiskThisYearSchema
                        :at-risk-last-year AtRiskLastYearSchema}
   :witan/output-schema {:births-pool BirthsPoolSchema}}
  [{:keys [at-risk-this-year at-risk-last-year]} _]
  (let [ds-joined (i/$join [[:gss-code :sex :age] [:gss-code :sex :age]]
                           at-risk-last-year
                           at-risk-this-year)]
    (hash-map :births-pool
              (-> ds-joined
                  (ds/add-column :birth-pool (i/$map (fnil (fn [this-yr last-yr]
                                                             (double (/ (+ this-yr last-yr) 2)))
                                                           0 0)
                                                     [:popn-this-yr :popn-last-yr] ds-joined))
                  (ds/remove-columns [:popn-this-yr :popn-last-yr])))))

(defn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  [inputs params]
  (-> (->births-data-year inputs)
      ->at-risk-this-year
      ->at-risk-last-year
      ->births-pool))
