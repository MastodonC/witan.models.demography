(ns witan.models.data-config)

(def default-bucket    "witan-data")
(def default-profile   "witan")
(def default-directory "datasets/default_datasets")
(def default-folder    "witan.models.demography")

(defn valid?
  [filename-regex file]
  (and (not (.isDirectory file))
       (re-find filename-regex (str file))))
