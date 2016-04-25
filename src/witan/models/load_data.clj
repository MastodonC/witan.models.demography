(ns witan.models.load-data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]))

(defn- custom-keyword [coll]
  (mapv #(-> %
             (clojure.string/replace #"[.]" "-")
             keyword) coll))

(defn- load-csv
  "Loads csv file with each row as a vector. 
   Stored in map separating column-names from data"
  ([filename]
   (load-csv filename nil))
  ([filename eol]
   (when (.exists (io/as-file filename))
     (let [normalised-data (-> (slurp filename)
                               (str/replace "\r\n" "\n")
                               (str/replace "\r" "\n"))
           parsed-csv (csv/parse-csv normalised-data :end-of-line eol)
           parsed-data (rest parsed-csv)
           headers (map str/lower-case (first parsed-csv))]
       {:column-names (custom-keyword headers)
        :columns (vec parsed-data)}))))

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


(def MyeCoCSchema {:column-names [(s/one (s/eq :gss-code) ":gss-code")
                                  (s/one (s/eq :district) ":district")
                                  (s/one (s/eq :sex) ":sex")
                                  (s/one (s/eq :age) ":age")
                                  (s/one (s/eq :var) ":var")
                                  (s/one (s/eq :year) ":year")
                                  (s/one (s/eq :estimate) ":estimate")]
                   :columns [(s/one [s/Str] "col gss-code")
                             (s/one [s/Str] "col district")
                             (s/one [s/Str] "col sex")
                             (s/one [s/Int] "col age")
                             (s/one [s/Str] "col var")
                             (s/one [s/Int] "col year")
                             (s/one [s/Num] "col estimate")]
                   s/Keyword s/Any})

(defn make-row-schema
  [col-schema]
  (mapv (fn [s] (let [datatype (-> s :schema first)
                      fieldname (:name s)]
                  (s/one datatype fieldname)))
        (:columns col-schema)))

(defn make-col-names-schema
  [col-schema]
  (mapv (fn [s] (let [datatype (:schema s)
                      fieldname (:name s)]
                  (s/one datatype fieldname)))
        (:column-names col-schema)))

(defn record-coercion
  "Coerce numbers by matching them to the
    type specified in the schema"
  [schema data]
  (let [coerce-data-fn
        (coerce/coercer schema
                        coerce/string-coercion-matcher)]
    (coerce-data-fn data)))

(defn apply-row-schema
  [col-schema csv-data]
  (let [row-schema (make-row-schema col-schema)]
    (map #((partial (fn [s r] (record-coercion s r)) row-schema) %) (:columns csv-data))))

(defn apply-col-names-schema
  [col-schema csv-data]
  (let [col-names-schema (make-col-names-schema col-schema)]
    (record-coercion col-names-schema (:column-names csv-data))))

(defmulti apply-rec-coercion
  (fn [data-info csv-data]
    (:type  data-info)))

(defmethod apply-rec-coercion :default
  [data-info csv-data]
  nil)

(defmethod apply-rec-coercion :births-data
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsDataSchema csv-data)
   :columns (vec (apply-row-schema BirthsDataSchema csv-data))})

(defmethod apply-rec-coercion :at-risk-popn
  [data-info csv-data]
  {:column-names (apply-col-names-schema AtRiskPopnSchema csv-data)
   :columns (vec (apply-row-schema AtRiskPopnSchema csv-data))})

(defmethod apply-rec-coercion :mye-coc
  [data-info csv-data]
  {:column-names (apply-col-names-schema MyeCoCSchema csv-data)
   :columns (vec (apply-row-schema MyeCoCSchema csv-data))})

(defn dataset-after-coercion
  [{:keys [column-names columns]}]
  (ds/dataset column-names columns))

(defn load-datasets
  "Input should be a map with keys for each dataset and filepaths to csv
   files as the values. Output is a map of core.matrix datasets."
  [file-map]
  (->> file-map
       (mapv (fn [[k path]]
               (->> (apply-rec-coercion {:type k} (load-csv path))
                    (dataset-after-coercion)
                    (hash-map k))))
       (reduce merge)))
