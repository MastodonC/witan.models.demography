(ns witan.models.split-data
  (:require [clojure.string :as str]
            [amazonica.aws.s3 :as aws]
            [amazonica.aws.s3transfer :as aws-s3]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [witan.models.data-config :as dc]
            [me.raynes.fs :as fs])
  (:gen-class))

(defn write-new-file
  [{:keys [path data]}]
  (println "Writing" path)
  (with-open [out-file (io/writer path)]
    (data-csv/write-csv out-file data)))

(defn split-files-by-gss-code
  [files]
  (reduce (fn [a file]
            (let [parsed-csv (data-csv/read-csv (slurp file))
                  parsed-data (rest parsed-csv)
                  headers (map str/lower-case (first parsed-csv))
                  gss-code-idx (.indexOf headers "gss.code")]
              (if (>= gss-code-idx 0)
                (let [grouped-by-gss-code (group-by #(nth % gss-code-idx) parsed-data)
                      new-files (mapv (fn [[gss-code data]]
                                        (let [name   (str file)
                                              name'  (subs name 0 (- (count name) 4))
                                              name'' (str name' "_" gss-code ".csv")]
                                          {:path name''
                                           :data (concat [headers] data)})) grouped-by-gss-code)
                      _ (run! write-new-file new-files)
                      new-file-names (mapv :path new-files)]
                  (concat
                   (conj a (str file))
                   new-file-names))
                (conj a (str file))))) [] files))


(defn -main
  "Splits the data into GSS code"
  []
  (time
   (let [dir (clojure.java.io/file dc/default-directory)
         files (file-seq dir)
         valid-files (filter (partial dc/valid? #"[a-zA-Z]+.csv$") files)]
     (split-files-by-gss-code valid-files))))
