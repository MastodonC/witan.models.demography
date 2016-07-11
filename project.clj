(defproject witan.models.demography "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [prismatic/schema "1.1.0"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [net.mikera/core.matrix "0.51.0"]
                 [kixi/incanter-core "1.9.1-p0-3bf644a"]
                 [witan.workspace-api "0.1.6"]]
  :target-path "target/%s"
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :dependencies [[commons-codec "1.10"]
                                  [witan.workspace "0.1.1" :exclusions [org.clojure/clojure clj-kafka commons-codec prismatic/schema]]
                                  [org.onyxplatform/onyx "0.9.4" :exclusions [commons-codec]]]}
             :uberjar {:aot :all}}
  :exclusions [prismatic/schema org.clojure/clojure])
