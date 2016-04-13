(defproject witan.models.demography "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [prismatic/schema "1.0.4"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [net.mikera/core.matrix "0.51.0"]]
  :main ^:skip-aot witan.models.demography
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
