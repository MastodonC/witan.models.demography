(ns witan.models.workspace-test
  (:require  [clojure.test :refer :all]
             [schema.core :as s]
             [clojure.core.async :refer [chan >!! <!! close!]]
             [onyx.plugin.core-async :refer [take-segments!]]
             [onyx.api]
             [witan.workspace.onyx :as o]
             [witan.workspace-api :refer [defworkflowfn]]
             [witan.workspace-api.onyx :refer [default-fn-wrapper
                                               default-pred-wrapper]]
             [witan.models.load-data :refer [load-dataset]]
             ;;
             [witan.models.dem.ccm.fert.fertility-mvp]
             [witan.models.dem.ccm.mort.mortality-mvp]
             [witan.models.dem.ccm.mig.net-migration]
             [witan.models.dem.ccm.core.projection-loop]))

(defn workspace
  [{:keys [workflow contracts catalog] :as raw}]
  (->
   raw
   (assoc :workflow (or workflow []))
   (assoc :contracts (or contracts []))
   (assoc :catalog (or catalog []))))

(def config
  {:redis-config {:redis/uri "redis"}
   :batch-settings {:onyx/batch-size 1
                    :onyx/batch-timeout 1000}
   :fn-wrapper :witan.workspace-api.onyx/default-fn-wrapper
   :pred-wrapper :witan.workspace-api.onyx/default-pred-wrapper})

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def id (java.util.UUID/randomUUID))

(def ccm-batch-size 1)

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/tenancy-id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx/tenancy-id id
   :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def capacity 1000)

(def output-chan (chan capacity))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

;; Executing before batch
(defn log-before-batch
  [event lifecycle]
  (println "Executing before batch" (:lifecycle/task lifecycle))
  {})

;; Executing after batch
(defn log-after-batch
  [event lifecycle]
  (println "Executing after batch" (:lifecycle/task lifecycle))
  {})

(def log-calls
  {:lifecycle/before-batch log-before-batch
   :lifecycle/after-batch log-after-batch})

(def lifecycles
  [{:lifecycle/task :out
    :lifecycle/calls :witan.models.workspace-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def catalog-entry-out
  {:onyx/name :out
   :onyx/plugin :onyx.plugin.core-async/output
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/batch-size ccm-batch-size
   :onyx/max-peers 1
   :onyx/doc "Writes segments to a core.async channel"})

(def tasks
  {:proj-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/project-asfr-finalyrhist-fixed
                                :params {:fert-last-yr 2014
                                         :start-yr-avg-fert 2014
                                         :end-yr-avg-fert 2014}}
   :join-popn-latest-yr        {:var #'witan.models.dem.ccm.core.projection-loop/join-popn-latest-yr}
   :add-births                 {:var #'witan.models.dem.ccm.core.projection-loop/add-births}
   :project-deaths             {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-deaths-from-fixed-rates}
   :proj-domestic-in-migrants  {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-in-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   ;;:incr-year                  {:var nil}
   :calc-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/calc-historic-asmr}
   :proj-domestic-out-migrants {:var #'witan.models.dem.ccm.mig.net-migration/project-domestic-out-migrants
                                :params {:start-yr-avg-dom-mig 2003
                                         :end-yr-avg-dom-mig 2014}}
   :remove-deaths              {:var #'witan.models.dem.ccm.core.projection-loop/remove-deaths}
   :age-on                     {:var #'witan.models.dem.ccm.core.projection-loop/age-on}
   :project-births             {:var #'witan.models.dem.ccm.fert.fertility-mvp/births-projection
                                :params {:proportion-male-newborns (double (/ 105 205))}}
   :proj-historic-asmr         {:var #'witan.models.dem.ccm.mort.mortality-mvp/project-asmr
                                :params {:start-yr-avg-mort 2010
                                         :end-yr-avg-mort 2014}}
   :select-starting-popn       {:var #'witan.models.dem.ccm.core.projection-loop/select-starting-popn}
   :calc-historic-asfr         {:var #'witan.models.dem.ccm.fert.fertility-mvp/calculate-historic-asfr
                                :params {:fert-last-yr 2014}}
   :apply-migration            {:var #'witan.models.dem.ccm.core.projection-loop/apply-migration}
   :proj-intl-in-migrants      {:var #'witan.models.dem.ccm.mig.net-migration/project-international-in-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   :proj-intl-out-migrants     {:var #'witan.models.dem.ccm.mig.net-migration/project-international-out-migrants
                                :params {:start-yr-avg-inter-mig 2003
                                         :end-yr-avg-inter-mig 2014}}
   :finish-looping?              {:var #'witan.models.dem.ccm.core.projection-loop/finish-looping?
                                  :params {:last-proj-year 2015}}})

(def inputs
  {:in-historic-popn                  {:src "test_data/model_inputs/bristol_hist_popn_mye.csv"
                                       :key :historic-population}
   :in-projd-births-by-age-of-mother  {:src "test_data/model_inputs/fert/bristol_ons_proj_births_age_mother.csv"
                                       :key :ons-proj-births-by-age-mother}
   :in-historic-total-births          {:src "test_data/model_inputs/fert/bristol_hist_births_mye.csv"
                                       :key :historic-births}
   :in-historic-deaths-by-age-and-sex {:src "test_data/model_inputs/mort/bristol_hist_deaths_mye.csv"
                                       :key :historic-deaths}
   :in-historic-dom-in-migrants       {:src "test_data/model_inputs/mig/bristol_hist_domestic_inmigrants.csv"
                                       :key :domestic-in-migrants}
   :in-historic-dom-out-migrants      {:src "test_data/model_inputs/mig/bristol_hist_domestic_outmigrants.csv"
                                       :key :domestic-out-migrants}
   :in-historic-intl-in-migrants      {:src "test_data/model_inputs/mig/bristol_hist_international_inmigrants.csv"
                                       :key :international-in-migrants}
   :in-historic-intl-out-migrants     {:src "test_data/model_inputs/mig/bristol_hist_international_outmigrants.csv"
                                       :key :international-out-migrants}})

(defmacro make-input-calls [& forms]
  `(do ~@(for [name forms]
           (let [namestr (subs (str name) 1)
                 sy-chan   (symbol (str namestr "-chan"))
                 sy-inject (symbol (str namestr "-inject-channel"))
                 sy-inputs (symbol (str namestr "-input-calls"))]
             `(do (def ~sy-chan   (chan 100))
                  (defn ~sy-inject [e# l#] (println "I AM STARTING") {:core.async/chan ~sy-chan})
                  (def ~sy-inputs {:lifecycle/before-task-start ~sy-inject
                                   :lifecycle/before-batch log-before-batch
                                   :lifecycle/after-batch log-after-batch}))))))

(make-input-calls
 :in-historic-popn
 :in-projd-births-by-age-of-mother
 :in-historic-total-births
 :in-historic-deaths-by-age-and-sex
 :in-historic-dom-in-migrants
 :in-historic-dom-out-migrants
 :in-historic-intl-in-migrants
 :in-historic-intl-out-migrants)

(defmacro input-to-lifecycle
  [input]
  `[{:lifecycle/task ~input
     :lifecycle/calls (keyword (str "witan.models.workspace-test/" (subs (str ~input) 1) "-input-calls"))}
    {:lifecycle/task ~input
     :lifecycle/calls :onyx.plugin.core-async/reader-calls}])

(defn input-to-catalog
  [batch-size input]
  {:onyx/name input
   :onyx/plugin :onyx.plugin.core-async/input
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/batch-size batch-size
   :onyx/max-peers 1
   :onyx/doc "Reads segments from a core.async channel"})

(def input-catalog
  (mapv (fn [[k _]] (input-to-catalog ccm-batch-size k)) inputs))

(def input-lifecycles
  (vec (mapcat #(input-to-lifecycle %) (keys inputs))))

(defn get-channel-from-kw
  [kw]
  (var-get (ns-resolve 'witan.models.workspace-test (symbol (str (name kw) "-chan")))))

(defn workflow-fn-to-catalog
  "Take a defworkflowfn var and convert to a witan workspace catalog entry"
  ([task-name var params]
   (let [{:keys [witan/doc witan/param-schema]} (or (get (meta var) :witan/workflowfn)
                                                    (get (meta var) :witan/workflowpred))]
     (merge
      (when params
        (if param-schema
          {:witan/params (s/validate param-schema params)}
          (throw (Exception. (str task-name " has params but no param-schema")))))
      {:witan/name task-name
       :witan/fn (-> var (str) (subs 2) (keyword))})))
  ([task-name var]
   (workflow-fn-to-catalog task-name var nil)))

(def ccm-catalog
  (mapv (fn [[k {:keys [var params]}]] (workflow-fn-to-catalog k var params)) tasks))

(def ccm-workflow
  [;; inputs for asfr
   [:in-historic-popn                 :calc-historic-asfr]
   [:in-historic-total-births         :calc-historic-asfr]
   [:in-projd-births-by-age-of-mother :calc-historic-asfr]


   ;; inputs for asmr
   [:in-historic-popn                  :calc-historic-asmr]
   [:in-historic-total-births          :calc-historic-asmr]
   [:in-historic-deaths-by-age-and-sex :calc-historic-asmr]

   ;; inputs for mig
   [:in-historic-dom-in-migrants   :proj-domestic-in-migrants]
   [:in-historic-dom-out-migrants  :proj-domestic-out-migrants]
   [:in-historic-intl-in-migrants  :proj-intl-in-migrants]
   [:in-historic-intl-out-migrants :proj-intl-out-migrants]

   ;; asfr/asmr projections
   [:calc-historic-asfr :proj-historic-asfr]
   [:calc-historic-asmr :proj-historic-asmr]

   ;; pre-loop merge
   [:proj-historic-asfr         :select-starting-popn]
   [:proj-historic-asmr         :select-starting-popn]
   [:proj-domestic-in-migrants  :select-starting-popn]
   [:proj-domestic-out-migrants :select-starting-popn]
   [:proj-intl-in-migrants      :select-starting-popn]
   [:proj-intl-out-migrants     :select-starting-popn]
   ;; - inputs for popn
   [:in-historic-popn           :select-starting-popn]

   ;; --- start popn loop
   [:select-starting-popn :project-births]
   [:select-starting-popn :project-deaths]
   [:project-births       :age-on]
   [:age-on               :add-births]
   [:add-births           :remove-deaths]
   [:project-deaths       :remove-deaths]
   [:remove-deaths        :apply-migration]
   [:apply-migration      :join-popn-latest-yr]
   [:join-popn-latest-yr  [:finish-looping? :out :select-starting-popn]]
   ;; --- end loop
   ])

(deftest testing-a-model
  (let [onyx-job (o/workspace->onyx-job
                  (workspace
                   {:workflow ccm-workflow
                    :catalog ccm-catalog}) config)
        onyx-job' (-> onyx-job
                      (assoc :lifecycles lifecycles)
                      (update :lifecycles (comp vec concat) input-lifecycles)
                      (update :catalog conj catalog-entry-out)
                      (update :catalog (comp vec concat) input-catalog))]
    (clojure.pprint/pprint onyx-job')

    (testing "Run the model"
      (doseq [[k {:keys [src key]}] inputs]
        (let [ch (get-channel-from-kw k)]
          (println k ">" src "into" key)
          (>!! ch (load-dataset key src))
          (>!! ch :done)
          (close! ch)))

      (let [env        (onyx.api/start-env env-config)
            peer-group (onyx.api/start-peer-group peer-config)
            n-peers    (count (set (mapcat identity ccm-workflow)))
            v-peers    (onyx.api/start-peers n-peers peer-group)
            _          (onyx.api/submit-job peer-config onyx-job')
            _ (println "Job has been submit")
            results    (take-segments! output-chan)
            _ (println "Results are in")]

        (doseq [v-peer v-peers]
          (onyx.api/shutdown-peer v-peer))
        (onyx.api/shutdown-peer-group peer-group)
        (onyx.api/shutdown-env env)

        (clojure.pprint/pprint (keys results))
        #_(is (= results [{:number 1 :foo 1}
                          {:number 2 :foo 2}
                          {:number 3 :foo 3}
                          {:number 4 :foo 4}
                          {:number 5 :foo 5}
                          {:number 6 :foo 6}
                          :done]))))))
