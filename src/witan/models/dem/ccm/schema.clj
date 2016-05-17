(ns witan.models.dem.ccm.schema
  (:require [schema.core :as s]))

;;Functions for creating schemas
;;From load-data ns
(defn make-ordered-ds-schema [col-vec]
  {:column-names (mapv #(s/one (s/eq (first %)) (str (first %))) col-vec)
   :columns (mapv #(s/one [(second %)] (format "col %s" (name (first %)))) col-vec)
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

;;Define schemas 
;;From load-data ns - changed schema name to include 'Schema'      
(def HistBirthsEstSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:district s/Str] [:sex s/Str] [:age s/Int]
                           [:var s/Str] [:year s/Int] [:estimate s/Num]]))   

;;From projection-loop ns
(def PopulationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int]
                           [:year s/Int] [:popn s/Int]]))

;;New schemas for fert/mort/mig inputs
(def BirthsBySexSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:births s/Num]]))

(def DeathsSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:death-rate s/Num] [:popn s/Int] [:deaths s/Num]]))

(def NetMigrationSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:net-mig s/Num]]))

;;From hist-asfr-ws ns
(def AtRiskThisYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:popn-this-yr s/Num] [:age s/Int]]))

(def AtRiskLastYearSchema
  (make-ordered-ds-schema [[:gss-code s/Str] [:sex s/Str] [:age s/Int] [:year s/Int]
                           [:popn-last-yr s/Num]]))

(def BirthsPoolSchema
  (make-ordered-ds-schema [[:age s/Int] [:sex s/Str] [:year (s/maybe s/Int)] [:gss-code s/Str]
                           [:birth-pool s/Num]]))
