(ns witan.models.run-models
  (:require [clojure.string :as str]
            [clojure.walk :as walk]
            [clojure.java.io :as jio]
            [clojure.core.matrix.dataset :as ds]
            [incanter.core :as i]
            [clojure.data.csv :as data-csv]
            [witan.models.load-data :as ld]
            [witan.models.dem.ccm.core.projection-loop :as core]
            [witan.datasets :as wds]
            [clojure.tools.cli :refer [parse-opts]]
            [taoensso.timbre :as timbre]
            [schema.core :as s]
            [witan.models.dem.ccm.models-utils :refer :all]
            [witan.models.dem.ccm.models :refer [cohort-component-model]]
            [witan.workspace-executor.core :as wex]
            [witan.workspace-api :refer [defworkflowfn]])
  (:gen-class :main true))

;; Handle inputs
(defn customise-headers [coll]
  (mapv #(-> %
             (clojure.string/replace #"[. /']" "-")
             keyword) coll))

(defn get-data-for-local-authority
  "Filters a dataset for a local authority.
   Takes in a filepath for a CSV file and a gss code.
   Returns a dataset for the local authority of interest."
  [dataset-path gss-code]
  (let [parsed-csv (data-csv/read-csv (slurp dataset-path))
        parsed-data (rest parsed-csv)
        headers (customise-headers (map clojure.string/lower-case
                                        (first parsed-csv)))
        index (.indexOf headers :gss-code)
        filtered-data (filterv #(= gss-code (nth % index)) parsed-data)]
    {:column-names headers
     :columns filtered-data}))

(defn get-dataset
  "Input is a keyword and a filepath to csv file
   Output is map with keyword and core.matrix dataset"
  [keyname filepath gss-code]
  (->> (ld/apply-record-coercion {:type keyname}
                                 (get-data-for-local-authority filepath gss-code))
       ld/create-dataset-after-coercion
       (hash-map keyname)))

(defworkflowfn resource-csv-loader-filtered
  "Loads CSV files from resources"
  {:witan/name :workspace-test/resource-csv-loader-filtered
   :witan/version "1.0.0"
   :witan/input-schema {:* s/Any}
   :witan/output-schema {:* s/Any}
   :witan/param-schema {:gss-code s/Str
                        :src s/Str
                        :key s/Keyword}}
  [_ {:keys [src key gss-code]}]
  (get-dataset key src gss-code))

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
   (let [parsed-csv (data-csv/read-csv (slurp filename) :end-of-line eol)
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
        order-columns (map #(order-result-map % sorted-colnames) rows-as-maps)
        sort-rows (sort-by #(vec (map % [:year :sex :age])) order-columns)]
    (with-open [out-file (jio/writer filepath)]
      (data-csv/write-csv out-file
                          (convert-row-maps-to-vector-of-vectors sort-rows)))))

(def local-authorities (read-string (slurp "./datasets/default_datasets/local_authorities.edn")))

(defn get-district
  [gss-code]
  (get-in local-authorities
          [:gss-code-to-district
           (keyword gss-code)]))

(defn add-district-to-dataset-per-user-input
  [data gss-code]
  (let [data-code (distinct (i/$ :gss-code data))]
    (when (and (= 1 (count data-code))
               (= gss-code (first data-code)))
      (ds/add-column data :district
                     (repeat (get-district gss-code))))))

;; Building the workspace:
(defn tasks [inputs params gss-code]
  { ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Functions
   :project-asfr               {:var #'witan.models.dem.ccm.fert.fertility/project-asfr-finalyrhist-fixed
                                :params {:fert-base-yr (:fert-base-yr params)}}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality/project-deaths-from-fixed-rates}
   :proj-dom-in-migrants       {:var #'witan.models.dem.ccm.mig.migration/project-domestic-in-migrants
                                :params {:start-yr-avg-domin-mig (:start-yr-avg-domin-mig params)
                                         :end-yr-avg-domin-mig (:end-yr-avg-domin-mig params)}}
   :calc-hist-asmr             {:var #'witan.models.dem.ccm.mort.mortality/calc-historic-asmr}
   :proj-dom-out-migrants      {:var #'witan.models.dem.ccm.mig.migration/project-domestic-out-migrants
                                :params {:start-yr-avg-domout-mig (:start-yr-avg-domout-mig params)
                                         :end-yr-avg-domout-mig (:end-yr-avg-domout-mig params)}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility/project-births-from-fixed-rates}
   :combine-into-births-by-sex {:var #'witan.models.dem.ccm.fert.fertility/combine-into-births-by-sex
                                :params {:proportion-male-newborns
                                         (:proportion-male-newborns params)}}
   :project-asmr               {:var #'witan.models.dem.ccm.mort.mortality/project-asmr-average-fixed
                                :params {:start-yr-avg-mort (:start-yr-avg-mort params)
                                         :end-yr-avg-mort (:end-yr-avg-mort params)}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :prepare-starting-popn      {:var #'witan.models.dem.ccm.core.projection-loop/prepare-inputs}
   :calc-hist-asfr             {:var #'witan.models.dem.ccm.fert.fertility/calculate-historic-asfr
                                :params {:fert-base-yr (:fert-base-yr params)}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.migration/project-international-in-migrants
                                :params {:start-yr-avg-intin-mig (:start-yr-avg-intin-mig params)
                                         :end-yr-avg-intin-mig (:end-yr-avg-intin-mig params)}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.migration/project-international-out-migrants
                                :params {:start-yr-avg-intout-mig (:start-yr-avg-intout-mig params)
                                         :end-yr-avg-intout-mig (:end-yr-avg-intout-mig params)}}

   :combine-into-net-flows {:var #'witan.models.dem.ccm.mig.migration/combine-into-net-flows}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Predicates
   :finish-looping?            {:var #'witan.models.dem.ccm.core.projection-loop/finished-looping?
                                :params {:last-proj-year (:last-proj-year params)}
                                :pred? true}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Inputs
   :in-hist-popn                  {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:historic-population inputs)
                                            :key :historic-population}}
   :in-hist-total-births          {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:historic-births inputs)
                                            :key :historic-births}}
   :in-proj-births-by-age-of-mother  {:var #'witan.models.run-models/resource-csv-loader-filtered
                                      :params {:gss-code gss-code
                                               :src (:ons-proj-births-by-age-mother inputs)
                                               :key :ons-proj-births-by-age-mother}}
   :in-hist-deaths-by-age-and-sex {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:historic-deaths inputs)
                                            :key :historic-deaths}}
   :in-hist-dom-in-migrants       {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:domestic-in-migrants inputs)
                                            :key :domestic-in-migrants}}
   :in-hist-dom-out-migrants      {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:domestic-out-migrants inputs)
                                            :key :domestic-out-migrants}}
   :in-hist-intl-in-migrants      {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:international-in-migrants inputs)
                                            :key :international-in-migrants}}
   :in-hist-intl-out-migrants     {:var #'witan.models.run-models/resource-csv-loader-filtered
                                   :params {:gss-code gss-code
                                            :src (:international-out-migrants inputs)
                                            :key :international-out-migrants}}
   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
   ;; Outputs
   :out {:var #'witan.models.dem.ccm.core.projection-loop/population-out}})

(defn run-workspace
  [inputs gss-code params]
  (let [tasks (tasks inputs params gss-code)
        workspace     {:workflow  (:workflow cohort-component-model)
                       :catalog   (make-catalog tasks)
                       :contracts (make-contracts tasks)}
        workspace'    (s/with-fn-validation (wex/build! workspace))
        result        (wex/run!! workspace' {})]
    (:population (first result))))

(def cli-options
  [["-i" "--input-config FILEPATH" "Filepath for the config file with inputs info"
    :validate [#(.exists (jio/file %)) "Must be an existing file path"]
    :default "./default_config.edn"]
   ["-o" "--output-projections FILEPATH" "Filepath for the output projections"
    :validate [#(.exists (jio/as-file (.getParent (jio/file %))))
               "Must contain an existing path of directories"]
    :default "./ccm_projections.csv"]
   ["-c" "--gss-code for a English local authority" "Gss code for the local authority of interest"]])

(defn get-inputs
  "Gets content out of an edn file
  with locations of data inputs."
  [input]
  (cond (string? input) (when (.exists (jio/as-file input))
                          (try (read-string (slurp input))
                               (catch Exception e (timbre/info (format "Caught Error: %s"
                                                                       (.getMessage e))))))
        (map? input) input))

(defn -main
  "runs the ccm, adds district information for easier human
   reading and outputs to a csv"
  [& args]
  (let [{:keys [errors options]} (parse-opts args cli-options)
        {:keys [input-config output-projections gss-code]} options
        {:keys [input-datasets user-parameters]} (get-inputs input-config)
        params (try (assoc user-parameters
                           :proportion-male-newborns
                           (double (/ (:number-male-newborns user-parameters)
                                      (:number-all-newborns user-parameters))))
                    (catch Exception e (timbre/info (format "Caught Error: %s"
                                                            (.getMessage e)))))]
    (timbre/info (format "\n%d option validation errors\n%s" (count errors)
                         (clojure.string/join "\n" errors)))
    (timbre/info (format "\nInput used: %s\nOutput used: %s\n"
                         (:input-config (:options (parse-opts args cli-options)))
                         (:output-projections (:options (parse-opts args cli-options)))))
    (timbre/info (format "\nPreparing projection for %s... " (get-district gss-code)))
    (-> input-datasets
        (run-workspace gss-code params)
        (add-district-to-dataset-per-user-input gss-code)
        (write-data-to-csv output-projections [:gss-code :district :sex :age :year :popn]))))

(comment
  "example gss code input when running on the repl"
  (-main "-cE06000023"))
