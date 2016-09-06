(ns witan.models.dem.ccm.models-test
  (:require [witan.models.dem.ccm.models :as m]
            [witan.workspace-api.protocols :as p]
            [schema.core :as s]
            [clojure.test :refer :all]))

;; We don't need to test whether catalog entries appear in the workflow as
;; that's done by the API:
;; https://github.com/MastodonC/witan.workspace-api/blob/master/src/witan/workspace_api/schema.clj#L14
;;
;; However, this checks to see if the functions exposed by the model-library
;; interface provide everything necessary to fulfil all the models that are
;; also exported.

(deftest validate-models
  (let [library (m/model-library)
        funs    (p/available-fns library)]
    (doseq [{:keys [catalog metadata workflow]} (p/available-models library)]
      (let [{:keys [witan/name witan/version]} metadata
            model-name (str name " " version)]
        (testing (str "Is this model valid? - " model-name)
          (is catalog)
          (is metadata)
          (doseq [{:keys [witan/name witan/fn witan/version witan/params]} catalog]
            (testing (str "\n> testing catalog entry " name " " version)
              (let [fnc (some #(when (and (= fn (:witan/name %))
                                          (= version (:witan/version %))) %) funs)]
                (is fnc) ;; if fail, can't find function with this name + version
                ;; only check 'function' types for params
                (when (and fnc (= (:witan/type fnc) :function))
                  (let [{:keys [witan/param-schema]} fnc]
                    (when (or params param-schema)
                      (is params)
                      (is param-schema)
                      (is (not (s/check param-schema params))))))))))))))
