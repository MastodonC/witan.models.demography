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

(defn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  [{births-data :births-data at-risk-popn :denominators mye-coc :mye-coc}
   {fert-last-yr :fert-last-yr}])
