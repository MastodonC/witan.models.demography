(ns witan.models.functions
  (:require [schema.core :as s]
            [witan.workspace-api :refer [defworkflowfn
                                         defworkflowpred]]))

(defworkflowpred gte-ten
  "true if number is greater than 10"
  {:witan/name :witan.test-preds/gte-ten
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}}
  [{:keys [number] :as msg} _]
  (>= number 10))

(defworkflowfn my-inc
  "increments a number"
  {:witan/name :witan.test-funcs/inc
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number] :as x} _]
  {:number (inc number)})

(defworkflowfn inc*
  "increments a number"
  {:witan/name :witan.test-funcs/inc*
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/output-schema {:foo s/Num}}
  [{:keys [number]} _]
  {:foo (inc number)})

(defworkflowfn mul2
  "multiplies a number by 2"
  {:witan/name :witan.test-funcs/mul2
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number]} _]
  {:number (* 2 number)})

(defworkflowfn mul2*
  "multiplies a number by 2"
  {:witan/name :witan.test-funcs/mul2*
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/output-schema {:mult s/Num}}
  [{:keys [number]} _]
  {:mult (* 2 number)})

(defworkflowfn mulX
  "multiplies a number by X"
  {:witan/name :witan.test-funcs/mulX
   :witan/version "1.0"
   :witan/input-schema {:number s/Num}
   :witan/param-schema {:x s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number] :as a} {:keys [x] :as b}]
  {:number (* number x)})

(defworkflowfn add
  "Adds number and to-add"
  {:witan/name :witan.test-funcs/add
   :witan/version "1.0"
   :witan/input-schema {:number s/Num
                        :to-add s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [number to-add]} _]
  {:number (+ number to-add)})

(defworkflowfn add*
  "Adds number and to-add"
  {:witan/name :witan.test-funcs/add
   :witan/version "1.0"
   :witan/input-schema {:foo s/Num
                        :mult s/Num}
   :witan/output-schema {:number s/Num}}
  [{:keys [foo mult]} _]
  {:number (+ foo mult)})

(defworkflowfn ->str
  "Converts thing to string"
  {:witan/name :witan.test-funcs/->str
   :witan/version "1.0"
   :witan/input-schema {:thing s/Any}
   :witan/output-schema {:out-str s/Any}}
  [{:keys [thing]} _]
  {:out-str (str thing)})

(defworkflowfn dupe
  "This is a test function which will duplicate a key
  into another key"
  {:witan/name :_
   :witan/version "1.0"
   :witan/input-schema {:* s/Any}
   :witan/param-schema {:from s/Keyword
                        :to s/Keyword}
   :witan/output-schema {:* s/Any}}
  [m {:keys [from to]}]
  (hash-map to (get m from)))
