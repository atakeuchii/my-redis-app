(ns my-redis-app.day2.atomicity
  "Day 2: 原子性が壊れる場所。在庫・レートリミッタ・ジョブキュー。"
  (:require [my-redis-app.bench :as bench]
            [my-redis-app.db :as db]
            [my-redis-app.redis :as r]))

;; ---------- 在庫（DB 版） ----------

(defn reset-stock-db!
  "在庫を指定数に戻す。"
  [item-id n]
  (db/q "UPDATE items SET stock = ? WHERE item_id = ?" n item-id))

(defn stock-db [item-id]
  (:stock (db/q1 "SELECT stock FROM items WHERE item_id = ?" item-id)))

(defn buy-naive!
  "素朴な購入。読んで、判断して、書く。
   成功したら :ok、在庫切れなら :sold-out。"
  [item-id]
  (let [current (stock-db item-id)]                    ; ① 読む
    (if (pos? current)
      (do (db/q "UPDATE items SET stock = ? WHERE item_id = ?"    ; ② 書く
                (dec current) item-id)
          :ok)
      :sold-out)))

(defn buy-sql-atomic!
  "SQL 1 文で原子的に減らす。条件を WHERE に入れる。"
  [item-id]
  (let [n (-> (db/q "UPDATE items SET stock = stock - 1
                     WHERE item_id = ? AND stock > 0"
                    item-id)
              first
              :next.jdbc/update-count)]
    (if (pos? n) :ok :sold-out)))

;; ---------- 在庫（Redis 版） ----------

(defn- stock-key [item-id] (str "stock:" item-id))

(defn reset-stock-redis!
  [pool item-id n]
  (r/cmd pool "SET" (stock-key item-id) n))

(defn stock-redis [pool item-id]
  (when-let [v (r/cmd pool "GET" (stock-key item-id))]
    (Long/parseLong v)))

(defn buy-redis-naive!
  "Redis を使うが、素朴に読んで書く。DB 版と同じく壊れる。"
  [pool item-id]
  (let [k (stock-key item-id)
        current (Long/parseLong (r/cmd pool "GET" k))]
    (if (pos? current)
      (do (r/cmd pool "SET" k (dec current)) :ok)
      :sold-out)))

(defn buy-redis-decr!
  "DECR してマイナスなら INCR で戻す。"
  [pool item-id]
  (let [k (stock-key item-id)
        remaining (r/cmd pool "DECR" k)]
    (if (>= remaining 0)
      :ok
      (do (r/cmd pool "INCR" k) :sold-out))))

;; ---------- レートリミッタ ----------

(def ^:const rate-limit 10)
(def ^:const rate-window 60)

(defn- rate-key [user-id] (str "rate:" user-id))

(defn allow-naive?
  "INCR してから EXPIRE する。この 2 コマンドの間に落ちると永久ブロック。"
  [pool user-id]
  (let [k (rate-key user-id)
        n (r/cmd pool "INCR" k)]
    (when (= n 1)
      (r/cmd pool "EXPIRE" k rate-window))     ; ← ここで落ちたら TTL が付かない
    (<= n rate-limit)))

(defn allow-naive-crash!
  "INCR の直後にクラッシュする版。EXPIRE が実行されない。"
  [pool user-id]
  (let [k (rate-key user-id)
        n (r/cmd pool "INCR" k)]
    (when (= n 1)
      (throw (ex-info "プロセスがクラッシュした" {})))   ; EXPIRE の前に死ぬ
    (<= n rate-limit)))

(defn allow-atomic?
  "SET NX EX で初期化と TTL を同時に行う。
   初回: SET k 1 NX EX 60 が成功 → 1 回目として許可
   2回目以降: SET が失敗 → INCR で数える"
  [pool user-id]
  (let [k (rate-key user-id)]
    (if (= "OK" (r/cmd pool "SET" k 1 "NX" "EX" rate-window))
      true                                    ; 初回。必ず許可
      (<= (r/cmd pool "INCR" k) rate-limit))))

(defn allow-sliding-self?
  "自作の実装で使えるコマンドだけでスライディングウィンドウを作る。
   ZREMRANGEBYSCORE が無いので ZRANGEBYSCORE + ZREM で代用する。"
  [pool user-id window-sec limit]
  (let [k      (str "rate:sw:" user-id)
        now    (System/currentTimeMillis)
        cutoff (- now (* window-sec 1000))]
    ;; 1. ウィンドウ外の記録を削除
    (let [old (r/cmd pool "ZRANGEBYSCORE" k "-inf" cutoff)]
      (when (seq old)
        (apply r/cmd pool "ZREM" k old)))
    ;; 2. 現在の数を数える
    (let [n (r/cmd pool "ZCARD" k)]
      (if (< n limit)
        ;; 3. 今回を記録
        (do (r/cmd pool "ZADD" k now (str now "-" (rand-int 1000000)))
            (r/cmd pool "EXPIRE" k window-sec)
            true)
        false))))

(defn allow-fixed?
  [pool user-id window-sec limit]
  (let [k (str "rate:" user-id)]
    (if (= "OK" (r/cmd pool "SET" k 1 "NX" "EX" window-sec))
      true
      (<= (r/cmd pool "INCR" k) limit))))

;; ---------- ジョブキュー ----------

(def ^:const queue-key "jobs:pending")
(def ^:const processing-key "jobs:processing")

(defn enqueue!
  [pool job]
  (r/cmd pool "LPUSH" queue-key job))

(defn dequeue-naive!
  "RPOP で取り出す。取り出した瞬間、ジョブは Redis から消える。"
  [pool]
  (r/cmd pool "RPOP" queue-key))

(defn process-job!
  "ジョブを処理する。fail-rate の確率で例外を投げる（ワーカーのクラッシュを模擬）。"
  [job fail-rate processed]
  (when (< (rand) fail-rate)
    (throw (ex-info "ワーカーがクラッシュした" {:job job})))
  (swap! processed conj job)
  :done)

(defn run-worker-naive!
  "RPOP で取って処理する。処理中に落ちたらジョブは失われる。"
  [pool n fail-rate processed]
  (dotimes [_ n]
    (when-let [job (dequeue-naive! pool)]
      (try
        (process-job! job fail-rate processed)
        (catch Exception _ nil)))))          ; 落ちた。ジョブはどこにも無い

(defn dequeue-two-step!
  "RPOP してから別のリストに LPUSH する。2 コマンドなので隙間がある。"
  [pool]
  (when-let [job (r/cmd pool "RPOP" queue-key)]
    ;; ← ここでクラッシュしたらジョブが消える
    (r/cmd pool "LPUSH" processing-key job)
    job))

(defn ack!
  "処理が終わったジョブを処理中リストから消す。"
  [pool job]
  (r/cmd pool "LREM" processing-key 1 job))

(defn run-worker-two-step!
  [pool n fail-rate processed]
  (dotimes [_ n]
    (when-let [job (dequeue-two-step! pool)]
      (try
        (process-job! job fail-rate processed)
        (ack! pool job)                       ; 成功したら処理中から消す
        (catch Exception _ nil)))))           ; 失敗。処理中リストに残る


(comment
  (require '[my-redis-app.day2.atomicity :as d2] '[my-redis-app.bench :as bench]
           '[my-redis-app.redis :as r] :reload)

  (def item "i000")

  (defn test-buy [label buy-fn stock attempts threads]
    (d2/reset-stock-db! item stock)
    (let [results (atom {:ok 0 :sold-out 0})
          each    (quot attempts threads)]
      (->> (range threads)
           (map (fn [_] (future (dotimes [_ each]
                                  (swap! results update (buy-fn item) (fnil inc 0))))))
           doall
           (run! deref))
      (let [final (d2/stock-db item)]
        (println (format "%-16s 在庫 %2d → %3d  成功 %3d  売切 %3d  %s"
                         label stock final (:ok @results) (:sold-out @results)
                         (if (and (>= final 0) (= (:ok @results) (- stock final)))
                           "整合"
                           "★不整合★"))))))
  (test-buy "素朴（DB）" d2/buy-sql-atomic! 20 200 20)

  (def mine (r/pool 6380))
  (defn test-buy-redis [label buy-fn stock attempts threads]
    (d2/reset-stock-redis! mine item stock)
    (let [results (atom {:ok 0 :sold-out 0})
          each    (quot attempts threads)]
      (->> (range threads)
           (map (fn [_] (future (dotimes [_ each]
                                  (swap! results update (buy-fn mine item) (fnil inc 0))))))
           doall
           (run! deref))
      (let [final (d2/stock-redis mine item)]
        (println (format "%-20s 在庫 %2d → %4d  成功 %3d  売切 %3d  %s"
                         label stock final (:ok @results) (:sold-out @results)
                         (if (= (:ok @results) stock) "整合" "★不整合★"))))))
  (test-buy-redis "Redis 素朴" d2/buy-redis-naive! 20 200 20)

  (d2/reset-stock-redis! mine item 1000000)
  (bench/report "Redis DECR（1 回）" (bench/timed #(d2/buy-redis-decr! mine item) 1000))

  (r/cmd mine "DEL" (str "rate:" "u00001"))

  (doseq [_ (range 15)] (d2/allow-naive? mine "u00001"))
  (r/cmd mine "TTL" (str "rate:" "u00001"))

  (defn test-rate-limiter [pool allow-fn label window limit]
    (let [uid (str "test-" (rand-int 1000000))]
      (r/cmd pool "DEL" (str "rate:" uid) (str "rate:sw:" uid))
      ;; 境界問題のテスト
      (let [burst1 (count (filter true? (repeatedly (* 2 limit) #(allow-fn pool uid window limit))))]
        (Thread/sleep (+ 100 (* window 1000)))
        (let [burst2 (count (filter true? (repeatedly (* 2 limit) #(allow-fn pool uid window limit))))]
          (println (format "%-24s 境界: 1回目 %2d, 待機後 %2d" label burst1 burst2))))
      ;; 並行実行のテスト
      (let [uid2 (str "conc-" (rand-int 1000000))
            ok   (atom 0)]
        (->> (range 20)
             (map (fn [_] (future (dotimes [_ 5]
                                    (when (allow-fn pool uid2 window limit)
                                      (swap! ok inc))))))
             doall
             (run! deref))
        (println (format "%-24s 並行: 100 回中 %3d 回通過（上限 %d）%s"
                         label @ok limit (if (<= @ok limit) "" "  ★超過★"))))))
  (test-rate-limiter mine d2/allow-fixed?        "固定ウィンドウ" 2 10)
  (test-rate-limiter mine d2/allow-sliding-self? "スライディング" 2 10)

  (r/cmd mine "DEL" d2/queue-key d2/processing-key)
  
  (doseq [i (range 5)] (d2/enqueue! mine (str "job-" i)))
  (r/cmd mine "LRANGE" d2/queue-key 0 -1)
  
  (repeatedly 6 #(d2/dequeue-naive! mine))

  (defn test-queue [label worker-fn n-jobs fail-rate]
    (r/cmd mine "DEL" d2/queue-key d2/processing-key)
    (doseq [i (range n-jobs)] (d2/enqueue! mine (str "job-" i)))
    (let [processed (atom [])]
      (->> (range 5)
           (map (fn [_] (future (worker-fn mine (quot n-jobs 5) fail-rate processed))))
           doall
           (run! deref))
      (let [done      (count @processed)
            pending   (r/cmd mine "LLEN" d2/queue-key)
            in-flight (r/cmd mine "LLEN" d2/processing-key)
            lost      (- n-jobs done pending in-flight)]
        (println (format "%-24s 投入 %3d  完了 %3d  待機 %3d  処理中 %3d  消失 %3d %s"
                         label n-jobs done pending in-flight lost
                         (if (zero? lost) "" "  ★消失★"))))))
  
  (test-queue "RPOP（失敗率 0%）"  d2/run-worker-naive! 100 0.0)
  (test-queue "RPOP（失敗率 20%）" d2/run-worker-naive! 100 0.2)
  )
