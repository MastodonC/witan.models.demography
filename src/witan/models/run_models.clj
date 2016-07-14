(ns witan.models.run-models
  (:require [clojure.string :as str]
            [clojure-csv.core :as csv]
            [clojure.walk :as walk]
            [clojure.java.io :as jio]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [clojure.data.csv :as data-csv]
            [witan.models.load-data :as ld]
            [witan.models.dem.ccm.core.projection-loop :as core]))

(defn customise-headers [coll]
  (mapv #(-> %
             (clojure.string/replace #"[. /']" "-")
             keyword) coll))

(defn get-data-for-local-authority
  "Filters a dataset for a local authority.
   Takes in a filepath for a CSV file and a gss code.
   Returns a dataset for the local authority of interest."
  [dataset-path gss-code]
  (let [normalised-data (-> (slurp dataset-path)
                            (str/replace "\r\n" "\n")
                            (str/replace "\r" "\n"))
        parsed-csv (csv/parse-csv normalised-data :end-of-line nil)
        parsed-data (rest parsed-csv)
        headers (customise-headers (map clojure.string/lower-case
                                        (first parsed-csv)))
        filtered-data (->> parsed-data
                         (map #(clojure.walk/keywordize-keys (zipmap headers %1)))
                         (filterv #(= (:gss-code %) gss-code)))
        seq-filtered-data (map #(-> %
                                    vals
                                    vec) filtered-data)]
    {:column-names headers
     :columns seq-filtered-data}))

(defn get-dataset
  "Input is a keyword and a filepath to csv file
   Output is map with keyword and core.matrix dataset"
  [keyname filepath gss-code]
  (->> (ld/apply-record-coercion {:type keyname}
                                 (get-data-for-local-authority filepath gss-code))
       ld/create-dataset-after-coercion
       (hash-map keyname)))

(defn get-datasets
  "Input should be a map with keys for each dataset and filepaths to csv
   files as the values. Output is a map of core.matrix datasets."
  [file-map gss-code]
  (->> file-map
       (mapv (fn [[k path]] (get-dataset k path gss-code)))
       (reduce merge)))

;; Functions from witan.models.econscratch to write datasets
;; to CSV and specify the order of the columns:

(defn order-result-map
  "Takes in a map of data and a map with colnames order.
  Returns the map of ordered data."
  [result-map order]
  (into (sorted-map-by (fn [k1 k2]
                         (compare [(get order k1) k1]
                                  [(get order k2) k2])))
        result-map))

(defn get-sorted-colnames
  "Takes a filepath to a csv file and outputs a map with
  the rank for each column name based on the columns order in the file."
  ([filename]
   (get-sorted-colnames filename nil))
  ([filename eol]
   (let [normalised-data (-> (slurp filename)
                             (str/replace "\r\n" "\n")
                             (str/replace "\r" "\n"))
         parsed-csv (csv/parse-csv normalised-data :end-of-line nil)
         parsed-data (rest parsed-csv)
         headers (mapv keyword (first parsed-csv))]
     (zipmap headers
             (vec (range (count headers)))))))

(defn convert-row-maps-to-vector-of-vectors
  "Takes a sequence of maps and returns a vector of vectors where the first vector
   contains column names and the following vetors contain the value for each
   row. This is the format needed to save as csv using the write-to-csv function"
  [rows-as-maps]
  (let [colnames (mapv name (keys (first rows-as-maps)))
        rows (mapv #(vec (vals %)) rows-as-maps)]
    (into [colnames] rows)))

(defn write-data-to-csv
  "Takes a dataset or coll of maps, a filepath for the output csv and a filepath for a csv OR
  a vector (example [:column-1 :column-2 :column-3]) OR a map (example
  {:column-1 0 :column-2 1 :column 3: 2} to use for the columns order.
  Writes to csv file with the filepath provided, for example 'folder1/folder2/filename.csv'
  or 'filename.csv'"
  [data filepath ordered-colnames]
  (let [rows-as-maps (cond (i/dataset? data) (ds/row-maps data)
                           :else data)
        sorted-colnames (cond (string? ordered-colnames)
                              (get-sorted-colnames ordered-colnames)
                              (map? ordered-colnames)
                              ordered-colnames
                              (vector? ordered-colnames)
                              (zipmap ordered-colnames
                                      (vec (range (count ordered-colnames)))))
        ordered-data (map #(order-result-map % sorted-colnames) rows-as-maps)]
    (with-open [out-file (jio/writer filepath)]
      (data-csv/write-csv out-file
                          (convert-row-maps-to-vector-of-vectors ordered-data)))))

(defn run-ccm
  "This function will evolve as we build the jar.
   Takes in a map of datasets, a gss code and a map of parameters.
   Returns a dataset with historical data and projections."
  [inputs gss-code params]
  (core/looping-test (get-datasets inputs gss-code)
                     params))
