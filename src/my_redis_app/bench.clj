(ns my-redis-app.bench
  "計測用のヘルパ。各日で使い回す。")

(defn timed
  "f を n 回実行し、1 回あたりのミリ秒を返す。"
  ([f n] (timed f n (max 10 (quot n 10))))
  ([f n warmup]
   (dotimes [_ warmup] (f))
   (let [start (System/nanoTime)]
     (dotimes [_ n] (f))
     (/ (- (System/nanoTime) start) 1e6 n))))

(defn percentiles
  "f を n 回実行し、各回のミリ秒からパーセンタイルを返す。"
  ([f n] (percentiles f n (max 10 (quot n 10))))
  ([f n warmup]
   (dotimes [_ warmup] (f))
   (let [samples (vec (sort (repeatedly n #(let [s (System/nanoTime)]
                                             (f)
                                             (/ (- (System/nanoTime) s) 1e6)))))
         at (fn [p] (nth samples (min (dec n) (int (* n p)))))]
     {:p50 (at 0.5) :p90 (at 0.9) :p99 (at 0.99)
      :min (first samples) :max (peek samples)})))

(defn concurrently
  "f を threads 本のスレッドで each 回ずつ実行し、全体の経過ミリ秒を返す。"
  [f threads each]
  (let [start (System/nanoTime)]
    (->> (range threads)
         (map (fn [_] (future (dotimes [_ each] (f)))))
         doall
         (run! deref))
    (/ (- (System/nanoTime) start) 1e6)))

(defn report
  [label ms]
  (println (format "%-30s %8.3f ms" label ms)))
