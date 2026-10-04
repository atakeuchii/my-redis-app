(ns my-redis-app.day1.cache
  "Day 1: キャッシュ。まず Redis なしで動かし、問題を測る。"
  (:require [clojure.string :as str]
            [my-redis-app.bench :as bench]
            [my-redis-app.db :as db]
            [my-redis-app.redis :as r]))

;; ---------- DB 直撃 ----------

(def db-hits    (atom 0))
(def cache-hits (atom 0))
(def cache-miss (atom 0))

(defn reset-counters! []
  (reset! db-hits 0) (reset! cache-hits 0) (reset! cache-miss 0))

(defn stats []
  {:db @db-hits :hit @cache-hits :miss @cache-miss})

(defn fetch-profile-from-db
  "ユーザー 1 人のプロフィールを DB から引く。"
  [user-id]
  (swap! db-hits inc)
  (db/q1 "SELECT user_id, name, level FROM users WHERE user_id = ?" user-id))

(defn top-user-ids
  "スコア上位 n 人の user_id。ランキング画面の元データ。"
  [n]
  (mapv :user_id
        (db/q "SELECT user_id, max(score) AS best
               FROM plays GROUP BY user_id
               ORDER BY best DESC LIMIT ?" n)))

(defn render-ranking-direct
  "ランキング画面を作る。プロフィールは毎回 DB から引く。"
  [user-ids]
  (mapv (fn [uid]
          (let [p (fetch-profile-from-db uid)]
            {:user_id uid :name (:name p) :level (:level p)}))
        user-ids))

;; ---------- キャッシュ ----------

(def ^:const profile-ttl 60)

(defn- profile-key [user-id] (str "user:" user-id))

(defn- encode-profile
  "プロフィールを 1 つの文字列にする。素朴に区切り文字で。"
  [p]
  (when p (str (:name p) "\u0001" (:level p))))

(defn- decode-profile
  [user-id s]
  (when s
    (let [[nm lv] (str/split s #"\u0001")]
      {:user_id user-id :name nm :level (Long/parseLong lv)})))

(defn fetch-profile-cached
  "cache-aside。キャッシュを見て、無ければ DB から取って入れる。"
  [pool user-id]
  (let [k (profile-key user-id)]
    (if-let [cached (r/cmd pool "GET" k)]
      (do (swap! cache-hits inc)
          (decode-profile user-id cached))
      (do (swap! cache-miss inc)
          (let [p (fetch-profile-from-db user-id)]
            (when p
              (r/cmd pool "SET" k (encode-profile p) "EX" profile-ttl))
            p)))))

(defn render-ranking-cached
  [pool user-ids]
  (mapv (fn [uid]
          (let [p (fetch-profile-cached pool uid)]
            {:user_id uid :name (:name p) :level (:level p)}))
        user-ids))

(defn render-ranking-direct-bulk
  "キャッシュなし。ただし DB へのクエリは IN でまとめる。"
  [user-ids]
  (swap! db-hits inc)
  (let [ph   (clojure.string/join "," (repeat (count user-ids) "?"))
        rows (apply db/q
                    (str "SELECT user_id, name, level FROM users WHERE user_id IN (" ph ")")
                    user-ids)
        by-id (into {} (map (juxt :user_id identity)) rows)]
    (mapv (fn [uid]
            (let [p (by-id uid)]
              {:user_id uid :name (:name p) :level (:level p)}))
          user-ids)))

(defn fetch-profiles-cached
  "複数ユーザーのプロフィールを取る。
   キャッシュは MGET で 1 往復、ミスした分だけ DB から IN で取る。"
  [pool user-ids]
  (let [ks     (mapv profile-key user-ids)
        cached (apply r/cmd pool "MGET" ks)
        pairs  (map vector user-ids cached)
        hits   (into {} (keep (fn [[uid s]]
                                (when s [uid (decode-profile uid s)]))) pairs)
        misses (mapv first (remove (fn [[_ s]] s) pairs))]
    (swap! cache-hits + (count hits))
    (swap! cache-miss + (count misses))
    (let [fetched (when (seq misses)
                    (swap! db-hits + (count misses))
                    (let [ph (str/join "," (repeat (count misses) "?"))
                          rows (apply db/q
                                      (str "SELECT user_id, name, level FROM users WHERE user_id IN (" ph ")")
                                      misses)]
                      (into {} (map (juxt :user_id identity)) rows)))]
      ;; ミスした分をキャッシュに入れる
      (doseq [[uid p] fetched]
        (r/cmd pool "SET" (profile-key uid) (encode-profile p) "EX" profile-ttl))
      ;; 元の順序で返す
      (mapv (fn [uid] (or (hits uid) (fetched uid))) user-ids))))

(defn render-ranking-mget
  [pool user-ids]
  (mapv (fn [p] {:user_id (:user_id p) :name (:name p) :level (:level p)})
        (fetch-profiles-cached pool user-ids)))

;; ---------- ランキングのキャッシュ ----------

(def ^:const ranking-ttl 5)
(def ^:const ranking-key "ranking:top20")

(defn compute-ranking
  "ランキングを DB で計算する。10 万行の集計 + ソート。重い。"
  [n]
  (swap! db-hits inc)
  (db/q "SELECT p.user_id, u.name, u.level, max(p.score) AS best
         FROM plays p JOIN users u USING (user_id)
         GROUP BY p.user_id, u.name, u.level
         ORDER BY best DESC LIMIT ?" n))

(defn- encode-ranking
  "ランキングを 1 つの文字列にする。行を \\u0002、列を \\u0001 で区切る。"
  [rows]
  (clojure.string/join "\u0002"
                       (map (fn [{:keys [user_id name level best]}]
                              (clojure.string/join "\u0001" [user_id name level best]))
                            rows)))

(defn- decode-ranking
  [s]
  (when (seq s)
    (mapv (fn [line]
            (let [[uid nm lv best] (clojure.string/split line #"\u0001")]
              {:user_id uid :name nm
               :level (Long/parseLong lv) :best (Long/parseLong best)}))
          (clojure.string/split s #"\u0002"))))

(defn ranking-direct
  "キャッシュなし。毎回 DB で計算する。"
  [n]
  (compute-ranking n))

(defn ranking-cached
  "cache-aside。無ければ計算してキャッシュに入れる。"
  [pool n]
  (if-let [cached (r/cmd pool "GET" ranking-key)]
    (do (swap! cache-hits inc)
        (decode-ranking cached))
    (do (swap! cache-miss inc)
        (let [rows (compute-ranking n)]
          (r/cmd pool "SET" ranking-key (encode-ranking rows) "EX" ranking-ttl)
          rows))))

(defn watch-expiry
  "TTL が切れる瞬間に負荷が集中することを観察する。"
  [pool seconds]
  (r/cmd pool "FLUSHDB")
  (reset-counters!)
  (let [stop (atom false)
        log  (atom [])]
    ;; 20 スレッドが継続的にアクセス
    (let [workers (doall
                   (for [_ (range 20)]
                     (future
                       (while (not @stop)
                         (ranking-cached pool 20)))))]
      ;; 1 秒ごとに DB クエリ数を記録
      (dotimes [i seconds]
        (Thread/sleep 1000)
        (swap! log conj {:sec i :db @db-hits}))
      (reset! stop true)
      (run! deref workers))
    ;; 秒ごとの増分を出す
    (let [counts (mapv :db @log)]
      (println "秒ごとの DB クエリ数:")
      (doseq [[i [a b]] (map-indexed vector (partition 2 1 (cons 0 counts)))]
        (println (format "  %2d 秒: %3d 件" i (- b a)))))))

;; ---------- (A) 再計算ロック ----------

(def ^:const lock-ttl 10)

(defn ranking-locked
  "ミス時、ロックを取れた 1 人だけが計算する。
   取れなかった者は少し待ってキャッシュを見直す。"
  [pool n]
  (let [lock-key (str ranking-key ":lock")]
    (loop [attempt 0]
      (if-let [cached (r/cmd pool "GET" ranking-key)]
        (do (swap! cache-hits inc) (decode-ranking cached))
        (do
          (swap! cache-miss inc)
          (if (= "OK" (r/cmd pool "SET" lock-key "1" "NX" "EX" lock-ttl))
            ;; ロックを取れた: 自分が計算する
            (try
              (let [rows (compute-ranking n)]
                (r/cmd pool "SET" ranking-key (encode-ranking rows) "EX" ranking-ttl)
                rows)
              (finally (r/cmd pool "DEL" lock-key)))
            ;; 取れなかった: 待ってから見直す
            (if (< attempt 50)
              (do (Thread/sleep 10) (recur (inc attempt)))
              ;; 待ちきれなければ自分で計算する（フォールバック）
              (compute-ranking n))))))))

;; ---------- (B) TTL のばらつき ----------

(defn- jittered-ttl
  "base の ±jitter% の範囲でばらつかせる。"
  [base jitter-pct]
  (let [j (* base (/ jitter-pct 100.0))]
    (int (+ base (- (rand (* 2 j)) j)))))

(defn ranking-cached-jitter
  [pool n]
  (if-let [cached (r/cmd pool "GET" ranking-key)]
    (do (swap! cache-hits inc) (decode-ranking cached))
    (do (swap! cache-miss inc)
        (let [rows (compute-ranking n)]
          (r/cmd pool "SET" ranking-key (encode-ranking rows)
                 "EX" (jittered-ttl ranking-ttl 30))
          rows))))

;; ---------- (C) 論理期限 ----------

(defn- encode-with-expiry
  [rows logical-expire-at]
  (str logical-expire-at "\u0003" (encode-ranking rows)))

(defn- decode-with-expiry
  [s]
  (when (seq s)
    (let [[exp body] (clojure.string/split s #"\u0003" 2)]
      {:expire-at (Long/parseLong exp) :rows (decode-ranking body)})))

(defn ranking-early-refresh
  "物理 TTL より短い論理期限を持たせ、近づくほど高い確率で再計算する。
   期限切れを待たずに更新するので、全員がミスする瞬間が来ない。"
  [pool n]
  (let [now (System/currentTimeMillis)]
    (if-let [cached (r/cmd pool "GET" ranking-key)]
      (let [{:keys [expire-at rows]} (decode-with-expiry cached)
            remain (- expire-at now)
            ;; 残り時間が短いほど再計算の確率が上がる
            refresh? (or (neg? remain)
                         (< (rand) (- 1.0 (/ remain (* ranking-ttl 1000.0)))))]
        (if refresh?
          (do (swap! cache-miss inc)
              (let [rows (compute-ranking n)]
                (r/cmd pool "SET" ranking-key
                       (encode-with-expiry rows (+ now (* ranking-ttl 1000)))
                       "EX" (* 2 ranking-ttl))      ; 物理 TTL は長めに
                rows))
          (do (swap! cache-hits inc) rows)))
      (do (swap! cache-miss inc)
          (let [rows (compute-ranking n)]
            (r/cmd pool "SET" ranking-key
                   (encode-with-expiry rows (+ now (* ranking-ttl 1000)))
                   "EX" (* 2 ranking-ttl))
            rows)))))

(defn ranking-best
  "早期再計算 + ロック。再計算のタイミングを分散し、かつ 1 人に絞る。"
  [pool n]
  (let [now      (System/currentTimeMillis)
        lock-key (str ranking-key ":lock")]
    (if-let [cached (r/cmd pool "GET" ranking-key)]
      (let [{:keys [expire-at rows]} (decode-with-expiry cached)
            remain   (- expire-at now)
            refresh? (or (neg? remain)
                         (< (rand) (- 1.0 (/ remain (* ranking-ttl 1000.0)))))]
        (if (and refresh?
                 (= "OK" (r/cmd pool "SET" lock-key "1" "NX" "EX" lock-ttl)))
          ;; 再計算すると判断し、かつロックを取れた人だけが計算する
          (try
            (swap! cache-miss inc)
            (let [rows (compute-ranking n)]
              (r/cmd pool "SET" ranking-key
                     (encode-with-expiry rows (+ now (* ranking-ttl 1000)))
                     "EX" (* 2 ranking-ttl))
              rows)
            (finally (r/cmd pool "DEL" lock-key)))
          ;; それ以外は古い値をそのまま返す（待たない）
          (do (swap! cache-hits inc) rows)))
      ;; キャッシュが無い（初回のみ）
      (do (swap! cache-miss inc)
          (let [rows (compute-ranking n)]
            (r/cmd pool "SET" ranking-key
                   (encode-with-expiry rows (+ now (* ranking-ttl 1000)))
                   "EX" (* 2 ranking-ttl))
            rows)))))

;; ---------- 更新 ----------

(defn update-name-db!
  "DB のユーザー名を更新する。"
  [user-id new-name]
  (db/q "UPDATE users SET name = ? WHERE user_id = ?" new-name user-id))

(defn update-name-invalidate!
  "(A) DB を更新してキャッシュを消す。"
  [pool user-id new-name]
  (update-name-db! user-id new-name)
  (r/cmd pool "DEL" (profile-key user-id)))

(comment
  (require '[my-redis-app.day1.cache :as d1] '[my-redis-app.redis :as r]
           '[my-redis-app.bench :as bench] :reload)

  (def mine (r/pool 6380))
  (def top20 (d1/top-user-ids 20))

  (r/cmd mine "FLUSHDB")
  (d1/ranking-cached mine 20)
  (bench/report "ランキング（DB 直撃）" (bench/timed #(d1/ranking-direct 20) 30))
  (bench/report "ランキング（キャッシュ）" (bench/timed #(d1/ranking-cached mine 20) 500))

  ;; 同時アクセス
  (doseq [[label f] [["DB 直撃"   #(d1/ranking-direct 20)]
                     ["キャッシュ" #(d1/ranking-cached mine 20)]]]
    (r/cmd mine "FLUSHDB")
    (d1/reset-counters!)
    (let [ms (bench/concurrently f 20 10)]
      (println (format "%-12s %7.0f ms  %6.1f req/秒  %s"
                       label ms (/ 200000.0 ms) (d1/stats)))))

  ;; day1_cache.clj の ranking-ttl を 5 に変えて reload
  (d1/watch-expiry mine 20)

  ;; さらなる改善
  (defn compare-strategies []
    (doseq [[label f] [["素朴"       #(d1/ranking-cached mine 20)]
                       ["ロック"     #(d1/ranking-locked mine 20)]
                       ["早期再計算" #(d1/ranking-early-refresh mine 20)]]]
      (r/cmd mine "FLUSHDB")
      (d1/reset-counters!)
      (let [ms (bench/concurrently f 20 10)]
        (println (format "%-12s %6.0f ms  %6.1f req/秒  DB=%3d hit=%4d miss=%3d"
                         label ms (/ 200000.0 ms)
                         @d1/db-hits @d1/cache-hits @d1/cache-miss)))))
  (compare-strategies)

  (defn watch-strategy [label f seconds]
    (r/cmd mine "FLUSHDB")
    (d1/reset-counters!)
    (let [stop (atom false)
          log  (atom [])]
      (let [workers (doall (for [_ (range 20)]
                             (future (while (not @stop) (f)))))]
        (dotimes [i seconds]
          (Thread/sleep 1000)
          (swap! log conj @d1/db-hits))
        (reset! stop true)
        (run! deref workers))
      (println label (vec (map - @log (cons 0 @log))))))

  (watch-strategy "素朴  " #(d1/ranking-cached mine 20) 15)
  (watch-strategy "ロック" #(d1/ranking-locked mine 20) 15))
