# Python Learning (Docker)

このプロジェクトは「Node.js も Docker で動かす」前提で、
`Express + Prisma + PostgreSQL` との対比で Python を学ぶための土台です。

## このプロジェクトで固定する運用

- ローカルで `python` / `pip` は直接実行しない
- すべて `docker compose` 経由で実行する
- DB あり構成を標準にする（Prisma + PostgreSQL の感覚に寄せる）

## Node.js(Docker) との対応表

- `express` アプリ本体 ⇔ Python アプリ本体（FastAPI: `app/main.py`）
- `prisma schema + prisma client` ⇔ Python ORM 層（今後追加）
- `postgres` コンテナ ⇔ `postgres` コンテナ（同じ）
- `docker compose run --rm api npm run dev` ⇔ `docker compose run --rm app python -m app.main`
- `docker compose run --rm api npx prisma migrate dev` ⇔ `docker compose run --rm app <Python migration command>`

## 起動手順

1. ビルド

```bash
docker compose build
```

2. DB を起動（バックグラウンド）

```bash
docker compose up -d db
```

3. アプリ実行（FastAPI サーバー）

```bash
docker compose run --rm app
```

4. ヘルスチェック

```bash
curl http://localhost:8000/api/health
```

## よく使うコマンド（Node.js感覚）

- アプリ実行

```bash
docker compose run --rm app python -m app.main
```

- Python REPL（`node` 相当）

```bash
docker compose run --rm app python
```

- DB接続確認

```bash
docker compose exec db psql -U app -d app
```

## Formatter / Linter / Typecheck

- Node.js との比較:
  - Node.js では `prettier`（整形）+ `eslint`（lint）+ `tsc`（型チェック）を使うことが多い。
  - Python では `ruff format` + `ruff check` + `pyright` が、同じ責務分担に近い。
- PHP との比較:
  - PHP では `php-cs-fixer`（整形）+ `phpstan` や `psalm`（型・静的解析）を使うことが多い。
- Python でのやり方（このプロジェクト）:
  - `ruff format`（整形）
  - `ruff check`（lint）
  - `pyright`（メインの型チェック）
  - `mypy`（補助的な型チェック。段階的 strict 化と相性が良い）

```bash
# Formatter
docker compose run --rm --no-deps app ruff format app

# Linter
docker compose run --rm --no-deps app ruff check app

# Typecheck (Pyright: Node.js の tsc に近い位置づけ)
docker compose run --rm --no-deps app pyright

# Typecheck (mypy: PHP の phpstan/psalm 的な補助解析として併用)
docker compose run --rm --no-deps app mypy
```

## お題2: JSONログ整形と異常値検出（3言語比較）

- 入力データ: `data/api_logs_sample.jsonl`
- 期待値: `data/api_logs_expected.json`

```bash
# Python
docker compose run --rm --no-deps app python -m app.data_processing.log_preprocessing --input data/api_logs_sample.jsonl

# Node.js
docker run --rm -v "$PWD":/workspace -w /workspace node:20 node comparisons/topic2/log_preprocessing_node.js --input data/api_logs_sample.jsonl

# PHP
docker run --rm -v "$PWD":/workspace -w /workspace php:8.3-cli php comparisons/topic2/log_preprocessing_php.php --input data/api_logs_sample.jsonl
```

```bash
# 期待値との比較（例: Python）
diff -u <(jq -S . data/api_logs_expected.json) <(jq -S . /tmp/logs_py.json)
```

## お題3: 顧客・注文データ結合（JOIN相当, 3言語比較）

- 入力データ:
  - `data/customers_sample.csv`
  - `data/orders_sample.csv`
- 期待値: `data/customer_orders_expected.json`

```bash
# Python
docker compose run --rm --no-deps app python -m app.data_processing.customer_order_join --customers data/customers_sample.csv --orders data/orders_sample.csv

# Node.js
docker run --rm -v "$PWD":/workspace -w /workspace node:20 node comparisons/topic3/customer_order_join_node.js --customers data/customers_sample.csv --orders data/orders_sample.csv

# PHP
docker run --rm -v "$PWD":/workspace -w /workspace php:8.3-cli php comparisons/topic3/customer_order_join_php.php --customers data/customers_sample.csv --orders data/orders_sample.csv
```

```bash
# 期待値との比較（例: Python）
diff -u <(jq -S . data/customer_orders_expected.json) <(jq -S . /tmp/topic3_py.json)
```
