(defproject witan.models.demography "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [prismatic/schema "1.1.0"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [net.mikera/core.matrix "0.51.0"]
                 [witan.workspace-executor "0.1.3"]
                 [kixi/incanter-core "1.9.1-p0-3bf644a"]
                 [witan.workspace-api "0.1.0-SNAPSHOT"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :exclusions [prismatic/schema org.clojure/clojure])
