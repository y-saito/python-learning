import argparse
import json
import logging
from pathlib import Path
from typing import TypedDict

import pandas as pd

# このファイルは「お題7: ETLミニパイプライン」の Python 実装。
# Node.js/PHP でも同じ入力・同じ出力スキーマを返し、ETLの段階比較を行う。
# Python は pandas で Extract/Transform/Load を1本の流れとして書きやすい点が比較ポイント。


class ExtractStats(TypedDict):
    # 入力CSVの総行数
    input_records: int


class TransformStats(TypedDict):
    # Transform完了後の行数（不正日付除外後）
    transformed_records: int
    # order_date 不正で除外した件数
    dropped_invalid_order_date_count: int
    # customer_id を補完した件数
    filled_customer_id_count: int
    # quantity を補完した件数
    filled_quantity_count: int
    # unit_price を補完した件数
    filled_unit_price_count: int
    # quantity 補完値（中央値）
    quantity_fill_value: float | int
    # unit_price 補完値（中央値）
    unit_price_fill_value: float | int


class LoadStats(TypedDict):
    # 保存先Parquetパス
    output_path: str
    # 保存行数
    loaded_records: int


class EtlSummary(TypedDict):
    # ETL各段階の件数統計
    extract: ExtractStats
    transform: TransformStats
    load: LoadStats
    # 変換後データの売上合計
    total_sales: float | int


class CleanedOrderItem(TypedDict):
    # 注文ID
    order_id: str
    # 注文日（YYYY-MM-DD）
    order_date: str
    # 顧客ID
    customer_id: str
    # 商品名
    product: str
    # 数量
    quantity: int
    # 単価
    unit_price: float | int
    # 注文月（YYYY-MM）
    order_month: str
    # 明細売上
    line_total: float | int


class EtlResult(TypedDict):
    # ETL件数サマリ
    summary: EtlSummary
    # Load対象となった整形済みデータ（先頭3件）
    sample_cleaned_rows: list[CleanedOrderItem]


def normalize_number(value: float) -> float | int:
    """比較しやすいJSON数値表現へ正規化する。"""
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def setup_logging(debug: bool) -> None:
    """デバッグ有無に応じてログ設定を初期化する。"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def extract_orders(input_csv: str, debug: bool) -> tuple[pd.DataFrame, ExtractStats]:
    """Extract: CSVを読み込み、件数統計を返す。"""
    logger = logging.getLogger(__name__)

    # Python: pandas.read_csv で一括読込し、直後に DataFrame として扱える。
    # Node.js/PHP は配列行データをループで保持してから変換処理へ進むことが多い。
    raw_df = pd.read_csv(input_csv, dtype=str)
    if debug:
        logger.debug("--- Extract raw_df ---\n%s", raw_df.to_string(index=False))

    return raw_df, {"input_records": int(len(raw_df))}


def transform_orders(raw_df: pd.DataFrame, debug: bool) -> tuple[pd.DataFrame, TransformStats]:
    """Transform: 欠損補完・型変換・派生列追加を行う。"""
    logger = logging.getLogger(__name__)

    df = raw_df.copy()

    # 行程1: 文字列トリム。Node.js/PHP では trim を各行で呼ぶ実装になりやすい。
    for column in ["order_id", "order_date", "customer_id", "product", "quantity", "unit_price"]:
        df[column] = df[column].fillna("").astype(str).str.strip()

    # 行程2: 注文日を日付型へ変換し、不正日付は除外。
    # Python は to_datetime(errors="coerce") で不正値を NaT に統一できる。
    # Node.js/PHP は Dateパース結果の真偽判定をループ内で実施することが多い。
    df["order_date_dt"] = pd.to_datetime(df["order_date"], errors="coerce")
    invalid_date_mask = df["order_date_dt"].isna()
    dropped_invalid_order_date_count = int(invalid_date_mask.sum())
    df = df.loc[~invalid_date_mask].copy()

    # 行程3: 数値変換。
    df["quantity_num"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price_num"] = pd.to_numeric(df["unit_price"], errors="coerce")

    # 行程4: 補完値（中央値）を算出。
    # quantity は 0 以下を無効として補完対象にする。
    valid_quantity = df.loc[df["quantity_num"] > 0, "quantity_num"]
    quantity_fill_value = float(valid_quantity.median()) if not valid_quantity.empty else 1.0

    # unit_price は負数を無効として補完対象にする。
    valid_unit_price = df.loc[df["unit_price_num"] >= 0, "unit_price_num"]
    unit_price_fill_value = float(valid_unit_price.median()) if not valid_unit_price.empty else 0.0

    # 行程5: 欠損/不正の補完。Python はマスクを使って列単位で一括補完できる。
    # Node.js/PHP は if 分岐で行ごとに代入する実装が中心になる。
    customer_missing_mask = df["customer_id"].eq("")
    quantity_invalid_mask = df["quantity_num"].isna() | (df["quantity_num"] <= 0)
    unit_price_invalid_mask = df["unit_price_num"].isna() | (df["unit_price_num"] < 0)

    filled_customer_id_count = int(customer_missing_mask.sum())
    filled_quantity_count = int(quantity_invalid_mask.sum())
    filled_unit_price_count = int(unit_price_invalid_mask.sum())

    df.loc[customer_missing_mask, "customer_id"] = "UNKNOWN_CUSTOMER"
    df.loc[quantity_invalid_mask, "quantity_num"] = quantity_fill_value
    df.loc[unit_price_invalid_mask, "unit_price_num"] = unit_price_fill_value

    # 行程6: 型を最終形へ確定し、派生列を追加。
    df["order_date"] = df["order_date_dt"].dt.strftime("%Y-%m-%d")
    df["order_month"] = df["order_date_dt"].dt.strftime("%Y-%m")
    df["quantity"] = df["quantity_num"].round().astype(int)
    df["unit_price"] = df["unit_price_num"].astype(float)
    df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)

    cleaned_df = df[
        [
            "order_id",
            "order_date",
            "customer_id",
            "product",
            "quantity",
            "unit_price",
            "order_month",
            "line_total",
        ]
    ].sort_values(["order_date", "order_id"])

    if debug:
        logger.debug("--- Transform cleaned_df ---\n%s", cleaned_df.to_string(index=False))

    transform_stats: TransformStats = {
        "transformed_records": int(len(cleaned_df)),
        "dropped_invalid_order_date_count": dropped_invalid_order_date_count,
        "filled_customer_id_count": filled_customer_id_count,
        "filled_quantity_count": filled_quantity_count,
        "filled_unit_price_count": filled_unit_price_count,
        "quantity_fill_value": normalize_number(quantity_fill_value),
        "unit_price_fill_value": normalize_number(unit_price_fill_value),
    }
    return cleaned_df, transform_stats


def load_orders(cleaned_df: pd.DataFrame, output_parquet: str) -> LoadStats:
    """Load: 整形済みデータをParquetへ保存し、保存統計を返す。"""
    # Python は to_parquet で DataFrame を直接保存できる。
    # Node.js/PHP はスキーマ定義と列書込を明示的に行う必要がある場合が多い。
    output_path = Path(output_parquet)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned_df.to_parquet(output_path, index=False, compression=None)

    return {
        "output_path": str(output_path),
        "loaded_records": int(len(cleaned_df)),
    }


def build_result(input_csv: str, output_parquet: str, debug: bool) -> EtlResult:
    """ETL処理全体を実行し、比較用JSONを組み立てる。"""
    raw_df, extract_stats = extract_orders(input_csv, debug)
    cleaned_df, transform_stats = transform_orders(raw_df, debug)
    load_stats = load_orders(cleaned_df, output_parquet)

    sample_cleaned_rows: list[CleanedOrderItem] = []
    for _, row in cleaned_df.head(3).iterrows():
        sample_cleaned_rows.append(
            {
                "order_id": str(row["order_id"]),
                "order_date": str(row["order_date"]),
                "customer_id": str(row["customer_id"]),
                "product": str(row["product"]),
                "quantity": int(row["quantity"]),
                "unit_price": normalize_number(float(row["unit_price"])),
                "order_month": str(row["order_month"]),
                "line_total": normalize_number(float(row["line_total"])),
            }
        )

    result: EtlResult = {
        "summary": {
            "extract": extract_stats,
            "transform": transform_stats,
            "load": load_stats,
            "total_sales": normalize_number(float(cleaned_df["line_total"].sum())),
        },
        "sample_cleaned_rows": sample_cleaned_rows,
    }
    return result


def main() -> None:
    """CLIエントリーポイント。ETL結果JSONを標準出力へ出す。"""
    parser = argparse.ArgumentParser(
        description="orders CSV を ETL し、clean_orders.parquet を生成して結果をJSON出力します。"
    )
    parser.add_argument("--input", required=True, help="入力CSVファイルパス")
    parser.add_argument("--output", required=True, help="出力Parquetファイルパス")
    parser.add_argument("--debug", action="store_true", help="中間DataFrameをDEBUGログ出力")
    args = parser.parse_args()

    setup_logging(args.debug)
    result = build_result(args.input, args.output, args.debug)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
