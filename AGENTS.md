# Codex 作業ルール

## 作業ファイルの保存場所
- プロジェクト配下に必要なファイルを配置すること。例: ソースコードは `app/`、ドキュメントは `planning/`。
- CI/ツール設定などのメタ情報はプロジェクト直下または専用ディレクトリ（例: `.codex/`）に置く。

## ファイル名の命名ルール
- 日時を含める必要があるドキュメントは `report_YYYYMMDD_HHMMSS.md` の形式で `docs/` 配下に保存する。
- Docker/Compose/依存ファイルは既定名を使用する（`Dockerfile`, `docker-compose.yml`, `requirements.txt`, `.dockerignore`）。
- アプリケーションソースは `app/` 配下に配置する。

## コミットメッセージのルール
- Conventional Commits に従い、必ず型を付与する（例: `feat: ...`, `fix: ...`, `chore: ...`）。
- scope が必要な場合は `type(scope): message` 形式を用いる（例: `feat(api): add user endpoints`）。
- メッセージは簡潔に変更内容を表現する。

## 権限・コマンド利用のルール
- `sudo` はユーザーから明示的な許可がある場合のみ使用する。

## PHP ,Nodejsとの比較
- 本プロジェクトは、PHP ,Nodejsを使っているエンジニアがPythonの使い方を学ぶものになるため、各行程で、必ずPHP ,Nodejsでのやり方と比較して、Pythonでのやり方を説明すること。