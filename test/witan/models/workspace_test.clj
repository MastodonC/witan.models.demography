(ns witan.models.workspace-test
  (:require  [clojure.test :refer :all]
             [clojure.core.async :refer [chan >!! <!! close!]]
             [onyx.plugin.core-async :refer [take-segments!]]
             [onyx.api]
             [witan.workspace.onyx :as o]))

(defn workspace
  [{:keys [workflow contracts catalog] :as raw}]
  (->
   raw
   (assoc :workflow (or workflow []))
   (assoc :contracts (or contracts []))
   (assoc :catalog (or catalog []))))

(deftest run-workspace-test
  (testing "Run model as a workspace"
    (is (= {:workflow [[:in :inc]
                       [:inc :out]]
                                        ;   :contracts []
            :catalog [{:onyx/name :inc
                       :onyx/fn   :witan.workspace.function-catalog/my-inc
                       :onyx/type :function
                       :onyx/batch-size 1}]
            :flow-conditions []
            :lifecycles []
            :task-scheduler :onyx.task-scheduler/balanced}
           (o/workspace->onyx-job
            (workspace
             {:workflow [[:in :inc]
                         [:inc :out]]
              :catalog [{:witan/name :inc
                         :witan/fn :witan.workspace.function-catalog/my-inc
                         :witan/version "1.0"
                         :witan/inputs [{:witan/input-src-fn   :clojure.core/identity
                                         :witan/input-src-key  1
                                         :witan/input-dest-key :number}]}]})
            config)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def id (java.util.UUID/randomUUID))

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

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(def workflow
  [[:in :inc]
   [:inc :out]])

(def capacity 1000)

(def input-chan (chan capacity))

(def output-chan (chan capacity))

;; Execute before task
(defn inc-before-task
  [event lifecycle]
  (println "Executing before task")
  {})

;; Execute after task
(defn inc-after-task
  [event lifecycle]
  (println "Executing after task")
  {})

;; Executing before batch
(defn inc-before-batch
  [event lifecycle]
  (println "Executing before batch")
  {})

;; Executing after batch
(defn inc-after-batch
  [event lifecycle]
  (println "Executing after batch")
  {})

(defn inject-in-ch [event lifecycle]
  {:core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def inc-calls
  {:lifecycle/before-task-start inc-before-task
   :lifecycle/before-batch inc-before-batch
   :lifecycle/after-batch inc-after-batch
   :lifecycle/after-task-stop inc-after-task})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :witan.models.workspace-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :inc
    :lifecycle/calls :witan.models.workspace-test/inc-calls}
   {:lifecycle/task :out
    :lifecycle/calls :witan.models.workspace-test/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :inc
    :onyx/fn :witan.models.workspace-test/my-inc
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def input-segments
  [{:n 0}
   {:n 1}
   {:n 2}
   {:n 3}
   {:n 4}
   {:n 5}
   :done])

(deftest onyx-job
  (testing "Run local onyx job"

    (doseq [segment input-segments]
      (>!! input-chan segment))
    (close! input-chan)
    (let [env        (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          n-peers    (count (set (mapcat identity workflow)))
          v-peers    (onyx.api/start-peers n-peers peer-group)
          _          (onyx.api/submit-job
                      peer-config
                      {:catalog catalog :workflow workflow :lifecycles lifecycles
                       :task-scheduler :onyx.task-scheduler/balanced})
          results (take-segments! output-chan)]
      (clojure.pprint/pprint results)

      (doseq [v-peer v-peers]
        (onyx.api/shutdown-peer v-peer))
      (onyx.api/shutdown-peer-group peer-group)
      (onyx.api/shutdown-env env)

      (is (= results [{:n 1}
                      {:n 2}
                      {:n 3}
                      {:n 4}
                      {:n 5}
                      {:n 6}
                      :done])))))
