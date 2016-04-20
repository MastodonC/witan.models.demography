(ns witan.models.dem.ccm.fert.hist-asfr-age-test
  (:require [clojure.test :refer :all]
            [witan.models.dem.ccm.fert.hist-asfr-age :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]))


;; Load testing data
(defn- load-csv
  "Loads csv file as a sequence of maps, with column names as keywords"
  ([filename]
   (load-csv filename nil))
  ([filename eol]
   (when (.exists (clojure.java.io/as-file filename))
     (let [normalised-data (-> (slurp filename)
                               (str/replace "\r\n" "\n")
                               (str/replace "\r" "\n"))
           parsed-csv (csv/parse-csv normalised-data :end-of-line eol)
           parsed-data (rest parsed-csv)
           headers (map str/lower-case (first parsed-csv))]
       (map #(walk/keywordize-keys (zipmap headers %1)) parsed-data)))))

(def BirthsData
  {:gss.code s/Str
   :sex s/Str
   :age s/Int
   :births s/Num
   :year s/Int})

(def AtRiskPopn
  {:gss.code s/Str
   :sex s/Str
   s/Keyword s/Int})

(def MyeCoc
  {:age s/Int
   :year s/Int
   :estimate s/Int
   s/Keyword s/Str})

(defn record-coercion
  "Coerce numbers by matching them to the
    type specified in the schema"
  [schema data]
  (let [coerce-data-fn
        (coerce/coercer schema
                        coerce/string-coercion-matcher)]
    (coerce-data-fn data)))

(defmulti apply-rec-coercion
  (fn [data-info csv-data]
    (:type  data-info)))

(defmethod apply-rec-coercion :default
  [data-info csv-data]
  nil)

(defmethod apply-rec-coercion :births-data
  [data-info csv-data]
  (map #(record-coercion BirthsData %) csv-data))

(defmethod apply-rec-coercion :at-risk-popn
  [data-info csv-data]
  (map #(record-coercion AtRiskPopn %) csv-data))

(defmethod apply-rec-coercion :mye-coc
  [data-info csv-data]
  (map #(record-coercion MyeCoc %) csv-data))

(def data-inputs (->> {:births-data "resources/test_data/bristol_births_data.csv"
                       :at-risk-popn "resources/test_data/bristol_denominators.csv"
                       :mye-coc "resources/test_data/bristol_mye_coc.csv"}
                      (transduce (map (fn [[k path]]
                                        (hash-map k (ds/dataset (apply-rec-coercion
                                                                 {:type k}
                                                                 (load-csv path))))))
                                 merge)))

(def params {:fert-last-yr 2014})
;; End of input data handling

;; Functions take maps of all inputs/outputs from parent nodes in workflow
(def from-births-data-year (merge data-inputs
                                  (->births-data-year data-inputs)))
(def for-births-pool (merge from-births-data-year
                            (->at-risk-this-year from-births-data-year)
                            (->at-risk-last-year from-births-data-year)))

(def from-births-pool (merge for-births-pool
                             (->births-pool for-births-pool)))
;; End of data map creation
