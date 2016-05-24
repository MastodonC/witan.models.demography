(ns witan.models.load-data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
            [schema.core :as s]
            [clojure.core.matrix.dataset :as ds]
            [witan.models.dem.ccm.schemas :refer :all]))

(defn- custom-keyword [coll]
  (mapv #(-> %
             (clojure.string/replace #"[. /']" "-")
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

(defmulti apply-record-coercion
  (fn [data-info csv-data]
    (:type  data-info)))

(defmethod apply-record-coercion :default
  [data-info csv-data]
  nil)

(defmethod apply-record-coercion :births-data
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsDataSchema csv-data)
   :columns (vec (apply-row-schema BirthsDataSchema csv-data))})

(defmethod apply-record-coercion :at-risk-popn
  [data-info csv-data]
  {:column-names (apply-col-names-schema AtRiskPopnSchema csv-data)
   :columns (vec (apply-row-schema AtRiskPopnSchema csv-data))})

(defmethod apply-record-coercion :hist-births-est
  [data-info csv-data]
  {:column-names (apply-col-names-schema HistBirthsEstSchema csv-data)
   :columns (vec (apply-row-schema HistBirthsEstSchema csv-data))})

(defmethod apply-record-coercion :population
  [data-info csv-data]
  {:column-names (apply-col-names-schema PopulationSchema csv-data)
   :columns (vec (apply-row-schema PopulationSchema csv-data))})

(defmethod apply-record-coercion :hist-popn-estimates
  [data-info csv-data]
  {:column-names (apply-col-names-schema PopulationSchema csv-data)
   :columns (vec (apply-row-schema PopulationSchema csv-data))})

(defmethod apply-record-coercion :end-population
  [data-info csv-data]
  {:column-names (apply-col-names-schema PopulationSchema csv-data)
   :columns (vec (apply-row-schema PopulationSchema csv-data))})

(defmethod apply-record-coercion :births
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsBySexSchema csv-data)
   :columns (vec (apply-row-schema BirthsBySexSchema csv-data))})

(defmethod apply-record-coercion :deaths
  [data-info csv-data]
  {:column-names (apply-col-names-schema DeathsSchema csv-data)
   :columns (vec (apply-row-schema DeathsSchema csv-data))})

(defmethod apply-record-coercion :net-migration
  [data-info csv-data]
  {:column-names (apply-col-names-schema NetMigrationSchema csv-data)
   :columns (vec (apply-row-schema NetMigrationSchema csv-data))})

(defmethod apply-record-coercion :migration-estimates
  [data-info csv-data]
  {:column-names (apply-col-names-schema MigrationEstimatesSchema csv-data)
   :columns (vec (apply-row-schema MigrationEstimatesSchema csv-data))})

(defmethod apply-record-coercion :dom-in-averages
  [data-info csv-data]
  {:column-names (apply-col-names-schema DomInAverageSchema csv-data)
   :columns (vec (apply-row-schema DomInAverageSchema csv-data))})

(defn create-dataset-after-coercion
  [{:keys [column-names columns]}]
  (ds/dataset column-names columns))

(defn load-dataset
  "Input is a keyword and a filepath to csv file
   Output is map with keyword and core.matrix dataset"
  [keyname filepath]
  (->> (apply-record-coercion {:type keyname} (load-csv filepath))
       create-dataset-after-coercion
       (hash-map keyname)))

(defn load-datasets
  "Input should be a map with keys for each dataset and filepaths to csv
   files as the values. Output is a map of core.matrix datasets."
  [file-map]
  (->> file-map
       (mapv (fn [[k path]] (load-dataset k path)))
       (reduce merge)))
