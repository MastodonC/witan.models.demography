(ns witan.models.workspace-test
  (:require  [clojure.test :refer :all]
             [witan.workspace.onyx :as o]))

(defn workspace
  [{:keys [workflow contracts catalog] :as raw}]
  (->
   raw
   (assoc :workflow (or workflow []))
   (assoc :contracts (or contracts []))
   (assoc :catalog (or catalog []))))

(def config
  {:redis-config {:redis/uri "redis"}
   :batch-settings {:onyx/batch-size 1}})

(defn batch-size
  [c]
  (get-in c [:batch-settings :onyx/batch-size]))

(deftest run-workspace-test
  (testing "Run model as a workspace"
    (is (= {:workflow [[:in :inc]
                       [:inc :out]]
                                        ;   :contracts []
            :catalog [{:onyx/name :inc
                       :onyx/fn   :witan.workspace.function-catalog/my-inc
                       :onyx/type :function
                       :onyx/batch-size (batch-size config)}]
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
                         :witan/inputs [{:witan/input-src-fn   'witan.workspace.core-test/get-data
                                         :witan/input-src-key  1
                                         :witan/input-dest-key :number}]}]})
            config)))))
