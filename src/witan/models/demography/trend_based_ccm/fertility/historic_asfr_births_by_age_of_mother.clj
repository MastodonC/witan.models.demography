(ns witan.models.demography.trend-based-ccm.fertility.historic-asfr-births-by-age-of-mother)

(defn ->births-pool
  "Calculates birth pool as avg of at risk popn in births-data's max year & max year - 1
   Inputs:  * births-data dataset
            * at-risk-popn dataset
   Outputs: * dataset with cols gss-code, sex, age, year, birth-pool"
  [births-data at-risk-popn])

(defn ->historic-fertility
  "Calculates historic fertility rates using births by age of mother data
   Inputs:  * map of datasets that incl. births-data, denominators, mye-coc
            * map of parameters that incl. fert-last-yr
   Outputs: * map of datasets containing historic-fert (calculated historic fertility rates)"
  [{births-data :births-data at-risk-popn :denominators mye-coc :mye-coc}
   {fert-last-yr :fert-last-yr}]
  (let [births-pool (->births-pool births-data at-risk-popn)]))
