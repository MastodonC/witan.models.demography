(ns witan.models.dem.ccm.fert.hist-asfr-age
  (:require [clojure.string :as str]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [witan.workspace-api :refer [defworkflowfn merge->]]))

;; Schemas for data inputs/ouputs:
;; Automate schemas creation
(defn make-ordered-ds-schema [col-vec]
  {:column-names (mapv #(s/one (s/eq (first %)) (str (first %))) col-vec)
   :columns (mapv #(s/one [(second %)] (format "col %s" (name (first %)))) col-vec)
   s/Keyword s/Any})

(def BirthsDataSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:births s/Num] [:year s/Int]]))

(def AtRiskPopnSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn s/Num] [:actualyear s/Int] [:actualage s/Int]]))

(def AtRiskThisYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:popn-this-yr s/Num] [:age s/Int]]))

(def AtRiskLastYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn-last-yr s/Num]]))

(def BirthsPoolSchema
  (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year (s/maybe s/Int)] [:gss-code s/Str]
                           [:birth-pool s/Num]]))

;; Functions:
(defworkflowfn ->births-data-year
  "Takes births-data dataset
  Returns maximum value in Year column of births-data"
  {:witan/name :historic-births-data-yr
   :witan/version "1.0"
   :witan/input-schema {:births-data BirthsDataSchema}
   :witan/output-schema {:yr s/Int}}
  [{:keys [births-data]} _]
  {:yr (->> births-data
            (i/$ :year)
            (reduce max))})

(defworkflowfn ->at-risk-this-year
  "Filters ds for actualyear = yr, removes three columns
   and rename two columns."
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

(defworkflowfn ->at-risk-last-year
  "Filters ds for actualyear = yr - 1, removes three columns
  and rename one column."
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

(defworkflowfn ->births-pool
  "Calculates birth pool as avg of at risk popn in births-data's max year & max year - 1
  Inputs:  at-risk-this-year ds and at-risk-last year ds
  Outputs: dataset with cols gss-code, sex, age, year, birth-pool"
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

(defworkflowfn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  {:witan/name :hist-asfr
   :witan/version "1.0"
   :witan/input-schema {:births-data BirthsDataSchema
                        :at-risk-popn AtRiskPopnSchema}
   :witan/output-schema {:yr s/Int
                         :at-risk-this-year AtRiskThisYearSchema
                         :at-risk-last-year AtRiskLastYearSchema
                         :births-pool BirthsPoolSchema}
   :witan/exported? true}
  [inputs params]
  (-> (->births-data-year inputs)
      (merge-> ->at-risk-this-year
               ->at-risk-last-year)
      ->births-pool))
