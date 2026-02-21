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
- PHP との比較:
  - PHP では `php-cs-fixer`（整形）+ `phpstan` や `psalm`（型・静的解析）を使うことが多い。
- Python でのやり方（このプロジェクト）:
  - `ruff format`（整形）
  - `ruff check`（lint）
  - `mypy`（型チェック）

```bash
# Formatter
docker compose run --rm --no-deps app ruff format app

# Linter
docker compose run --rm --no-deps app ruff check app

# Typecheck
docker compose run --rm --no-deps app mypy
```
