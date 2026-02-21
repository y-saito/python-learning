import argparse
import json
import logging
from typing import Any, TypedDict

import pandas as pd

# このファイルは「お題6: JSON / Parquet 読み込み比較」の Python 実装。
# Node.js/PHP では JSON は標準機能で扱いやすい一方、Parquet は追加ライブラリが必要。
# Python では pandas.read_json / pandas.read_parquet で同じDataFrameに寄せられる点を確認する。


class DailySalesItem(TypedDict):
    # 集計日（YYYY-MM-DD）
    date: str
    # 日次売上
    sales: float | int


class CategorySalesItem(TypedDict):
    # カテゴリ名
    category: str
    # カテゴリ売上
    sales: float | int


class TopProductItem(TypedDict):
    # 商品名
    product: str
    # 商品売上
    sales: float | int
    # 商品数量
    quantity: int


class Aggregations(TypedDict):
    # 日次売上
    daily_sales: list[DailySalesItem]
    # カテゴリ売上
    category_sales: list[CategorySalesItem]
    # 商品売上Top3
    top_products: list[TopProductItem]


class DifferenceItem(TypedDict):
    # 差分が発生した配列インデックス
    index: int
    # JSON集計側の値
    json_value: Any
    # Parquet集計側の値
    parquet_value: Any


class Differences(TypedDict):
    # 日次売上差分
    daily_sales: list[DifferenceItem]
    # カテゴリ売上差分
    category_sales: list[DifferenceItem]
    # 商品Top3差分
    top_products: list[DifferenceItem]


class Summary(TypedDict):
    # JSON入力レコード件数
    json_record_count: int
    # Parquet入力レコード件数
    parquet_record_count: int
    # 3集計軸で完全一致しているか
    is_equivalent: bool


class DataFormatCompareResult(TypedDict):
    # 入力件数と一致判定サマリ
    summary: Summary
    # JSON入力由来の集計
    json_aggregations: Aggregations
    # Parquet入力由来の集計
    parquet_aggregations: Aggregations
    # 集計軸別の差分詳細
    differences: Differences


def normalize_number(value: float) -> float | int:
    """比較しやすいJSON数値表現へ正規化する。"""
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def aggregate_sales(df: pd.DataFrame) -> Aggregations:
    """入力DataFrameを日次・カテゴリ・商品Top3で集計する。"""
    # Python では列演算 + groupby を宣言的に書ける。
    # Node.js/PHP は Map/連想配列の更新ロジックを手続き的に書くことが多い。
    working_df = df.copy()
    working_df["line_total"] = working_df["quantity"] * working_df["price"]

    daily_df = (
        working_df.groupby("date", as_index=False)
        .agg(sales=("line_total", "sum"))
        .sort_values("date")
    )
    category_df = (
        working_df.groupby("category", as_index=False)
        .agg(sales=("line_total", "sum"))
        .sort_values(["sales", "category"], ascending=[False, True])
    )
    product_df = (
        working_df.groupby("product", as_index=False)
        .agg(sales=("line_total", "sum"), quantity=("quantity", "sum"))
        .sort_values(["sales", "product"], ascending=[False, True])
        .head(3)
    )

    daily_sales: list[DailySalesItem] = [
        {"date": str(row["date"]), "sales": normalize_number(float(row["sales"]))}
        for _, row in daily_df.iterrows()
    ]
    category_sales: list[CategorySalesItem] = [
        {"category": str(row["category"]), "sales": normalize_number(float(row["sales"]))}
        for _, row in category_df.iterrows()
    ]
    top_products: list[TopProductItem] = [
        {
            "product": str(row["product"]),
            "sales": normalize_number(float(row["sales"])),
            "quantity": int(row["quantity"]),
        }
        for _, row in product_df.iterrows()
    ]

    return {
        "daily_sales": daily_sales,
        "category_sales": category_sales,
        "top_products": top_products,
    }


def compare_items(json_items: list[Any], parquet_items: list[Any]) -> list[DifferenceItem]:
    """2つの配列をインデックス単位で比較し、差分のみ返す。"""
    differences: list[DifferenceItem] = []
    max_len = max(len(json_items), len(parquet_items))
    for index in range(max_len):
        json_value = json_items[index] if index < len(json_items) else None
        parquet_value = parquet_items[index] if index < len(parquet_items) else None
        if json_value != parquet_value:
            differences.append(
                {
                    "index": index,
                    "json_value": json_value,
                    "parquet_value": parquet_value,
                }
            )
    return differences


def build_result(json_input: str, parquet_input: str, debug: bool) -> DataFormatCompareResult:
    """JSON/Parquetを読み込み、集計比較結果を組み立てる。"""
    logger = logging.getLogger(__name__)

    # Python: pandas.read_json / pandas.read_parquet で同じDataFrame構造へ統一できる。
    # Node.js/PHP: JSONは標準関数で容易だが、Parquetは追加ライブラリ導入が必要になる。
    json_df = pd.read_json(json_input, convert_dates=False)
    parquet_df = pd.read_parquet(parquet_input)

    if debug:
        logger.debug("--- JSON DataFrame ---\n%s", json_df.to_string(index=False))
        logger.debug("--- Parquet DataFrame ---\n%s", parquet_df.to_string(index=False))

    json_aggregations = aggregate_sales(json_df)
    parquet_aggregations = aggregate_sales(parquet_df)

    differences: Differences = {
        "daily_sales": compare_items(
            json_aggregations["daily_sales"], parquet_aggregations["daily_sales"]
        ),
        "category_sales": compare_items(
            json_aggregations["category_sales"], parquet_aggregations["category_sales"]
        ),
        "top_products": compare_items(
            json_aggregations["top_products"], parquet_aggregations["top_products"]
        ),
    }

    is_equivalent = (
        len(differences["daily_sales"]) == 0
        and len(differences["category_sales"]) == 0
        and len(differences["top_products"]) == 0
    )

    summary: Summary = {
        # JSON入力の件数。
        "json_record_count": int(len(json_df)),
        # Parquet入力の件数。
        "parquet_record_count": int(len(parquet_df)),
        # 集計差分が0件かどうか。
        "is_equivalent": is_equivalent,
    }

    return {
        "summary": summary,
        "json_aggregations": json_aggregations,
        "parquet_aggregations": parquet_aggregations,
        "differences": differences,
    }


def setup_logging(debug: bool) -> None:
    """デバッグ指定時のみDEBUGログを有効にする。"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def main() -> None:
    """お題6 CLI のエントリーポイント。"""
    parser = argparse.ArgumentParser(
        description="JSON/Parquet を読み込み、同一集計結果を比較します。"
    )
    parser.add_argument("--json-input", required=True, help="入力JSONファイルパス")
    parser.add_argument("--parquet-input", required=True, help="入力Parquetファイルパス")
    parser.add_argument("--debug", action="store_true", help="中間DataFrameをDEBUGログ出力")
    args = parser.parse_args()

    setup_logging(args.debug)
    result = build_result(args.json_input, args.parquet_input, args.debug)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
