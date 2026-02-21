# Python Learning (Docker)

このプロジェクトは「Node.js も Docker で動かす」前提で、
`Express + Prisma + PostgreSQL` との対比で Python を学ぶための土台です。

## このプロジェクトで固定する運用

- ローカルで `python` / `pip` は直接実行しない
- すべて `docker compose` 経由で実行する
- DB あり構成を標準にする（Prisma + PostgreSQL の感覚に寄せる）

## Node.js(Docker) との対応表

- `express` アプリ本体 ⇔ Python アプリ本体（今は `app/main.py`）
- `prisma schema + prisma client` ⇔ Python ORM 層（今後追加）
- `postgres` コンテナ ⇔ `postgres` コンテナ（同じ）
- `docker compose run --rm api npm run dev` ⇔ `docker compose run --rm app python app/main.py`
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

3. アプリ実行

```bash
docker compose run --rm app
```

## よく使うコマンド（Node.js感覚）

- アプリ実行

```bash
docker compose run --rm app python app/main.py
```

- Python REPL（`node` 相当）

```bash
docker compose run --rm app python
```

- DB接続確認

```bash
docker compose exec db psql -U app -d app
```
