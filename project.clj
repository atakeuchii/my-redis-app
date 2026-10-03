(defproject my-redis-app "0.1.0-SNAPSHOT"
  :description "Redis 実践編：作った Redis を使う側から見る"
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [org.clojure/data.csv "1.1.0"]
                 [com.github.seancorfield/next.jdbc "1.3.955"]
                 [org.postgresql/postgresql "42.7.4"]]
  :global-vars {*warn-on-reflection* true}
  :repl-options {:init-ns my-redis-app.core})
