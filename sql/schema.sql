DROP TABLE IF EXISTS plays;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS items;

CREATE TABLE users (
  user_id       TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  level         INT  NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE plays (
  play_id   TEXT PRIMARY KEY,
  user_id   TEXT NOT NULL REFERENCES users(user_id),
  score     INT  NOT NULL,
  played_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE items (
  item_id TEXT PRIMARY KEY,
  name    TEXT NOT NULL,
  stock   INT  NOT NULL,
  price   INT  NOT NULL
);

-- Day 3 で「インデックスがあっても遅い」を見るために張っておく
CREATE INDEX idx_plays_user  ON plays(user_id);
CREATE INDEX idx_plays_score ON plays(score DESC);

-- マテリアライズドビュー
CREATE MATERIALIZED VIEW user_best AS
SELECT user_id, max(score) AS best FROM plays GROUP BY user_id;
CREATE INDEX ON user_best(best DESC);

REFRESH MATERIALIZED VIEW user_best;

CREATE TABLE IF NOT EXISTS purchases (
  purchase_id  TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL,
  item_id      TEXT NOT NULL,
  purchased_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_purchases_item ON purchases(item_id);
