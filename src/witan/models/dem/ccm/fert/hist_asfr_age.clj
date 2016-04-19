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
  [{:keys [births-data]}]
  "Takes births-data dataset
   Returns maximum value in Year column of births-data")

(defn at-risk-this-year
  "Filters ds for actualyear = yr, removes three columns
  and rename two columns."
  [at-risk-popn yr]
  (-> at-risk-popn
      (i/query-dataset {:actualyear yr})
      (ds/remove-columns [:age])
      (ds/rename-columns {:actualage :age :popn :popn-this-yr})
      (ds/remove-columns [:year :actualyear])))

(defn at-risk-last-year
  "Filters ds for actualyear = yr, removes three columns
  and rename one column."
  [at-risk-popn yr]
  (-> at-risk-popn
      (i/query-dataset {:actualyear yr})
      (ds/rename-columns {:popn :popn-last-yr})
      (ds/remove-columns [:age :actualage :actualyear])))

(defn ->births-pool
  "Calculates birth pool as avg of at risk popn in births-data's max year & max year - 1
   Inputs:  * births-data dataset
            * at-risk-popn dataset
   Outputs: * dataset with cols gss-code, sex, age, year, birth-pool"
  [at-risk-popn fert-last-yr]
  (let [ds-joined (i/$join [[:gss.code :sex] [:gss.code :sex]]
                           (at-risk-this-year at-risk-popn fert-last-yr)
                           (at-risk-last-year at-risk-popn (dec fert-last-yr)))]
    (ds/add-column ds-joined :birth-pool (i/$map (fn [this-yr last-yr]
                                                   (double (/ (+ this-yr last-yr) 2)))
                                                 [:popn-this-yr :popn-last-yr] ds-joined))))

(defn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  [{births-data :births-data at-risk-popn :denominators mye-coc :mye-coc}
   {fert-last-yr :fert-last-yr}])
