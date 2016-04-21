(ns witan.models.dem.ccm.fert.hist-asfr-age
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]))

(defn ->births-data-year
  "Takes births-data dataset
   Returns maximum value in Year column of births-data"
  [{:keys [births-data]}]
  {:yr (->> births-data ;; FIXME: change key to :births-data-year once using workflow
            (i/$ :year)
            (reduce max))})

(defn ->at-risk-this-year
  "Filters ds for actualyear = yr, removes three columns
  and rename two columns."
  [{:keys [at-risk-popn yr]}]
  {:at-risk-this-year (-> at-risk-popn
                          (i/query-dataset {:actualyear yr})
                          (ds/remove-columns [:age])
                          (ds/rename-columns {:actualage :age :popn :popn-this-yr})
                          (ds/remove-columns [:year :actualyear]))})

(defn ->at-risk-last-year
  "Filters ds for actualyear = yr - 1, removes three columns
  and rename one column."
  [{:keys [at-risk-popn yr]}]
  {:at-risk-last-year (-> at-risk-popn
                          (i/query-dataset {:actualyear (dec yr)})
                          (ds/rename-columns {:popn :popn-last-yr})
                          (ds/remove-columns [:actualage :actualyear]))})

(defn ->births-pool
  "Calculates birth pool as avg of at risk popn in births-data's max year & max year - 1
   Inputs:  at-risk-this-year ds and at-risk-last year ds
   Outputs: dataset with cols gss-code, sex, age, year, birth-pool"
  [{:keys [at-risk-this-year at-risk-last-year]}]
  (let [ds-joined (i/$join [[:gss.code :sex :age] [:gss.code :sex :age]]
                           at-risk-last-year
                           at-risk-this-year)
        ;; Replace `nil` by `NaN` in popn-last-yr column
        ds-joined-cleaned (ds/replace-column ds-joined :popn-last-yr
                                             (i/$map (fn [lst-yr-popn]
                                                       (if (nil? lst-yr-popn) (Double/NaN)
                                                           lst-yr-popn)) :popn-last-yr ds-joined))
        ds-bp-nan (-> ds-joined-cleaned
                      ;; Add a birth-pool column that averages popn-this-yr and popn-last-yr
                      (ds/add-column :birth-pool (i/$map (fn [this-yr last-yr]
                                                           (double (/ (+ this-yr last-yr) 2)))
                                                         [:popn-this-yr :popn-last-yr]
                                                         ds-joined-cleaned))
                      (ds/remove-columns [:popn-this-yr :popn-last-yr]))]
    (hash-map :births-pool ;; Replace `NaN` by 0.0 in the birth-pool column
              (ds/replace-column ds-bp-nan :birth-pool
                                 (i/$map (fn [bp]
                                           (if (Double/isNaN bp) 0.0 bp))
                                         :birth-pool ds-bp-nan)))))

(defn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  [{:keys [births-data at-risk-popn mye-coc]}
   {:keys [fert-last-yr]}])
