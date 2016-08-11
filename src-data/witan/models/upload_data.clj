(ns witan.models.upload-data
  (:require [clojure.string :as str]
            [amazonica.aws.s3 :as aws]
            [amazonica.aws.s3transfer :as aws-s3])
  (:gen-class))

(def default-bucket    "witan-data")
(def default-profile   "witan")
(def default-directory "datasets/default_datasets")
(def default-folder    "witan.models.demography")
(def filename-regex    #".csv")

(defn upload-file
  [dir bucket file]
  (let [name (->>
              (str/replace (str file) (str dir) "")
              (str default-folder))]
    (println "Uploading" (str file) "to" name)
    (aws/put-object {:profile default-profile}
                    :bucket-name bucket
                    :key name
                    :file file)))

(defn valid?
  [file]
  (and (not (.isDirectory file))
       (re-find filename-regex (str file))))

(defn -main
  "Uploads the data to S3"
  []
  (let [dir (clojure.java.io/file default-directory)
        files (file-seq dir)
        valid-files (filter valid? files)]
    (run! (partial upload-file dir default-bucket) valid-files)))
