(ns witan.models.load-data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure-csv.core :as csv]
            [schema.coerce :as coerce]
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
   (let [file (io/resource filename)]
     (when (.exists (io/as-file file))
       (let [normalised-data (-> (slurp file)
                                 (str/replace "\r\n" "\n")
                                 (str/replace "\r" "\n"))
             parsed-csv (csv/parse-csv normalised-data :end-of-line eol)
             parsed-data (rest parsed-csv)
             headers (map str/lower-case (first parsed-csv))]
         {:column-names (custom-keyword headers)
          :columns (vec parsed-data)})))))

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
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

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
  {:column-names (apply-col-names-schema DeathsOutputSchema csv-data)
   :columns (vec (apply-row-schema DeathsOutputSchema csv-data))})

(defmethod apply-record-coercion :net-migration
  [data-info csv-data]
  {:column-names (apply-col-names-schema NetMigrationSchema csv-data)
   :columns (vec (apply-row-schema NetMigrationSchema csv-data))})

(defmethod apply-record-coercion :migration-mye
  [data-info csv-data]
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

(defmethod apply-record-coercion :domestic-in-migrants
  [data-info csv-data]
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

(defmethod apply-record-coercion :domestic-out-migrants
  [data-info csv-data]
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

(defmethod apply-record-coercion :international-in-migrants
  [data-info csv-data]
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

(defmethod apply-record-coercion :international-out-migrants
  [data-info csv-data]
  {:column-names (apply-col-names-schema ComponentMYESchema csv-data)
   :columns (vec (apply-row-schema ComponentMYESchema csv-data))})

(defmethod apply-record-coercion :dom-in-averages
  [data-info csv-data]
  {:column-names (apply-col-names-schema DomInAverageSchema csv-data)
   :columns (vec (apply-row-schema DomInAverageSchema csv-data))})

(defmethod apply-record-coercion :dom-out-averages
  [data-info csv-data]
  {:column-names (apply-col-names-schema DomOutAverageSchema csv-data)
   :columns (vec (apply-row-schema DomOutAverageSchema csv-data))})

(defmethod apply-record-coercion :inter-in-averages
  [data-info csv-data]
  {:column-names (apply-col-names-schema InterInAverageSchema csv-data)
   :columns (vec (apply-row-schema InterInAverageSchema csv-data))})

(defmethod apply-record-coercion :inter-out-averages
  [data-info csv-data]
  {:column-names (apply-col-names-schema InterOutAverageSchema csv-data)
   :columns (vec (apply-row-schema InterOutAverageSchema csv-data))})

(defmethod apply-record-coercion :historic-deaths
  [data-info csv-data]
  {:column-names (apply-col-names-schema DeathsSchema csv-data)
   :columns (vec (apply-row-schema DeathsSchema csv-data))})

(defmethod apply-record-coercion :historic-births
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsBySexAgeYearSchema csv-data)
   :columns (vec (apply-row-schema BirthsBySexAgeYearSchema csv-data))})

(defmethod apply-record-coercion :historic-population
  [data-info csv-data]
  {:column-names (apply-col-names-schema HistPopulationSchema csv-data)
   :columns (vec (apply-row-schema HistPopulationSchema csv-data))})

(defmethod apply-record-coercion :historic-asmr
  [data-info csv-data]
  {:column-names (apply-col-names-schema HistASMRSchema csv-data)
   :columns (vec (apply-row-schema HistASMRSchema csv-data))})

(defmethod apply-record-coercion :historic-asfr
  [data-info csv-data]
  {:column-names (apply-col-names-schema HistASFRSchema csv-data)
   :columns (vec (apply-row-schema HistASFRSchema csv-data))})

(defmethod apply-record-coercion :projected-asfr-finalyrfixed
  [data-info csv-data]
  {:column-names (apply-col-names-schema ProjFixedASFRSchema csv-data)
   :columns (vec (apply-row-schema ProjFixedASFRSchema csv-data))})

(defmethod apply-record-coercion :births-by-age-sex-mother
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsAgeSexMotherSchema csv-data)
   :columns (vec (apply-row-schema BirthsAgeSexMotherSchema csv-data))})

(defmethod apply-record-coercion :births-by-sex
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsBySexSchema csv-data)
   :columns (vec (apply-row-schema  BirthsBySexSchema csv-data))})

(defmethod apply-record-coercion :historic-total-births
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsSchema csv-data)
   :columns (vec (apply-row-schema BirthsSchema csv-data))})

(defmethod apply-record-coercion :ons-proj-births-by-age-mother
  [data-info csv-data]
  {:column-names (apply-col-names-schema BirthsAgeSexMotherSchema csv-data)
   :columns (vec (apply-row-schema BirthsAgeSexMotherSchema csv-data))})

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
