# 作業報告書

- 作成日時: 2026-02-21 22:22:35
- 対象プロジェクト: `python-learning`
- 報告範囲: お題6（JSON / Parquet 読み込み比較）の実装・比較・検証

## 1. 実施サマリ

同一内容の `data/topic6_sales.json` と `data/topic6_sales.parquet` を入力にして、`Python` / `Node.js` / `PHP` の3実装を作成した。
3実装は共通CLI（`--json-input`, `--parquet-input`, `--debug`）で動作し、同一JSONスキーマ（`summary`, `json_aggregations`, `parquet_aggregations`, `differences`）を返す。

## 2. 追加・更新ファイル

1. 実装
- `app/data_processing/data_format_compare.py`
- `comparisons/topic6/data_format_compare_node.js`
- `comparisons/topic6/data_format_compare_php.php`

2. データ
- `data/topic6_sales.json`
- `data/topic6_sales.parquet`
- `data/topic6_format_expected.json`

3. 依存・ドキュメント
- `requirements.txt`
  - `pyarrow==17.0.0` を追加
- `README.md`
  - お題6の実行コマンド、期待値比較コマンドを追記

## 3. 共通仕様（I/F）

- 入力:
  - `--json-input data/topic6_sales.json`
  - `--parquet-input data/topic6_sales.parquet`
  - `--debug`（中間データを標準エラー出力）
- 出力キー:
  - `summary`
    - `json_record_count`
    - `parquet_record_count`
    - `is_equivalent`
  - `json_aggregations`
    - `daily_sales`
    - `category_sales`
    - `top_products`
  - `parquet_aggregations`
    - `daily_sales`
    - `category_sales`
    - `top_products`
  - `differences`
    - `daily_sales`
    - `category_sales`
    - `top_products`

## 4. 行程別比較（Node.js / PHP / Python）

### 行程1: データ読み込み（JSON / Parquet）
- Node.js:
  - JSONは `JSON.parse`、Parquetは `parquetjs-lite` を使って読み込み。
- PHP:
  - JSONは `json_decode`、Parquetは `codename/parquet`（`ParquetDataIterator`）を使って読み込み。
- Python:
  - `pandas.read_json(convert_dates=False)` と `pandas.read_parquet()` で読み込み。
- Python が得意な点:
  - ファイル形式差分を吸収して同じ DataFrame に寄せられるため、後続処理を共通化しやすい。

### 行程2: 共通集計（日次・カテゴリ・商品Top3）
- Node.js:
  - `Map` を更新して `line_total` を集計。
- PHP:
  - 連想配列を更新して `line_total` を集計。
- Python:
  - 列演算 + `groupby/agg/sort_values` で集計。
- Python が得意な点:
  - 集計軸や並び替え条件を宣言的に記述でき、仕様変更時の修正範囲が小さい。

### 行程3: 差分比較（JSON集計 vs Parquet集計）
- Node.js:
  - 配列をインデックス単位で比較し、差分オブジェクトを生成。
- PHP:
  - 配列をインデックス単位で比較し、差分オブジェクトを生成。
- Python:
  - 配列をインデックス単位で比較し、差分オブジェクトを生成。
- Python が得意な点:
  - 読み込みから比較まで同一データ構造で扱えるため、検証ロジックが一貫する。

## 5. 実行コマンド（Docker 経由）

1. Python
```bash
docker compose run --rm --no-deps app python -m app.data_processing.data_format_compare \
  --json-input data/topic6_sales.json \
  --parquet-input data/topic6_sales.parquet \
  > data/tmp/topic6_py.json
```

2. Node.js
```bash
docker run --rm -v "$PWD":/workspace -w /workspace node:20 sh -lc \
  "cd /tmp && npm init -y >/dev/null && npm install parquetjs-lite@0.8.7 >/dev/null && \
   NODE_PATH=/tmp/node_modules node /workspace/comparisons/topic6/data_format_compare_node.js \
   --json-input /workspace/data/topic6_sales.json \
   --parquet-input /workspace/data/topic6_sales.parquet" \
  > data/tmp/topic6_node.json
```

3. PHP
```bash
docker run --rm -v "$PWD":/workspace -w /workspace php:8.3-cli bash -lc \
  "apt-get update >/dev/null && apt-get install -y --no-install-recommends curl git unzip libgmp-dev >/dev/null && \
   docker-php-ext-install -j\$(nproc) gmp bcmath >/dev/null && \
   mkdir -p /tmp/topic6_php_vendor && cd /tmp/topic6_php_vendor && \
   printf '{\"name\":\"topic6/parquet\",\"require\":{}}' > composer.json && \
   curl -sS https://getcomposer.org/installer | php >/dev/null && \
   php composer.phar require codename/parquet:^0.7.2 --no-interaction >/dev/null && \
   PARQUET_VENDOR_AUTOLOAD=/tmp/topic6_php_vendor/vendor/autoload.php \
   php /workspace/comparisons/topic6/data_format_compare_php.php \
     --json-input /workspace/data/topic6_sales.json \
     --parquet-input /workspace/data/topic6_sales.parquet" \
  > data/tmp/topic6_php.json
```

4. 一致検証
```bash
diff -u <(jq -S . data/topic6_format_expected.json) <(jq -S . data/tmp/topic6_py.json)
diff -u <(jq -S . data/topic6_format_expected.json) <(jq -S . data/tmp/topic6_node.json)
diff -u <(jq -S . data/topic6_format_expected.json) <(jq -S . data/tmp/topic6_php.json)
```

## 6. 検証結果

- 3言語すべて `data/topic6_format_expected.json` と差分なし。
- `summary`:
  - `json_record_count`: 9
  - `parquet_record_count`: 9
  - `is_equivalent`: true
- `--debug` 指定時も3言語で最終JSONは期待値一致。

## 7. 品質チェック

- `docker compose run --rm --no-deps app ruff check app` -> `All checks passed!`
- `docker compose run --rm --no-deps app mypy app` -> `Success: no issues found in 11 source files`

## 8. お題6 完了判定

以下を満たしたため、お題6を完了とする。

1. 3言語で JSON / Parquet の実読込を実装した。
2. 同一CLI・同一JSONスキーマで出力を統一した。
3. 期待値比較（diff）と `--debug` 実行で整合性を確認した。
4. `ruff` と `mypy` を通過した。

## 9. 補足（Parquet互換の実装メモ）

- Node.js (`parquetjs-lite`) と PHP (`codename/parquet`) の互換性を優先するため、`data/topic6_sales.parquet` は無圧縮で作成した。
- 圧縮方式によっては PHP 側で `snappy` 拡張が必要になるため、学習用サンプルでは追加拡張なしで実行できる形式を採用した。
