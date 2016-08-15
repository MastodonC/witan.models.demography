(ns witan.models.upload-data
  (:require [clojure.string :as str]
            [amazonica.aws.s3 :as aws]
            [amazonica.aws.s3transfer :as aws-s3]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [witan.models.data-config :as dc]
            [me.raynes.fs :as fs])
  (:import java.util.zip.GZIPOutputStream)
  (:gen-class))

(defn gzip
  [input output & opts]
  (with-open [output (-> output io/output-stream GZIPOutputStream.)]
    (apply io/copy input output opts)))

(defn upload-file
  [dir bucket file]
  (let [tmpfile (fs/temp-file "ccm" ".txt.gz")
        _ (with-open [in (io/input-stream file)
                      out (io/output-stream tmpfile)]
            (gzip in out))
        name (->>
              (str/replace (str file) (str dir) "")
              (str dc/default-folder))
        name' (str name ".gz")]
    (println "Uploading" (str file) "to" name')
    (aws/put-object {:profile dc/default-profile}
                    :bucket-name bucket
                    :key name'
                    :file tmpfile)
    (fs/delete (str tmpfile))))

(defn -main
  "Uploads the data to S3"
  []
  (time
   (let [dir (clojure.java.io/file dc/default-directory)
         files (file-seq dir)
         valid-files (filter (partial dc/valid? #".csv$") files)]
     (run! (partial upload-file dir dc/default-bucket) valid-files))))
