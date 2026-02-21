import argparse
import json
import logging
from typing import TypedDict

import pandas as pd

# このファイルは「お題1: CSV売上集計」の Python 実装。
# 目的は、同じ入力CSVに対して Node.js/PHP と同一スキーマのJSONを返すこと。
# 集計の核は pandas の groupby/agg で、宣言的に処理を記述する。


# 以下の TypedDict 群は、出力JSONの「型付き仕様書」。
# Node.js であれば TypeScript の interface、PHP であれば配列仕様ドキュメントに相当。
# Python では mypy と組み合わせることで、キー名の打ち間違いや型崩れを検知できる。
class DailySalesItem(TypedDict):
    # 集計対象の日付（YYYY-MM-DD）
    date: str
    # その日の合計売上
    sales: float | int


class CategorySalesItem(TypedDict):
    # カテゴリ名（例: Food, Stationery）
    category: str
    # カテゴリ合計売上
    sales: float | int


class TopProductItem(TypedDict):
    # 商品名
    product: str
    # 商品別の合計売上
    sales: float | int
    # 商品別の合計数量
    quantity: int


class SalesAggregationResult(TypedDict):
    # 日次売上の配列
    daily_sales: list[DailySalesItem]
    # カテゴリ別売上の配列
    category_sales: list[CategorySalesItem]
    # 売上上位商品の配列（Top3）
    top_products: list[TopProductItem]


def money(value: float) -> float | int:
    """3言語比較で使う金額表現へ正規化する。

    Args:
        value: 正規化前の金額。

    Returns:
        小数第2位で丸めた値。小数部が0なら int に変換した値。
    """
    # 金額表現を3言語で揃えるための正規化関数。
    # 1. 小数第2位まで丸める（例: 10.555 -> 10.56）
    # 2. 末尾が .00 になる場合は整数に落とす（例: 25.0 -> 25）
    # Node.js 側の toFixed(2) + Number(...)、PHP 側の round と同じ目的。
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def aggregate_sales(input_path: str) -> SalesAggregationResult:
    """売上CSVを日次・カテゴリ別・商品上位の3観点で集計する。

    Args:
        input_path: 入力CSVファイルのパス。

    Returns:
        日次売上、カテゴリ売上、売上上位商品を含む集計結果。
    """
    _, daily_df, category_df, product_df = build_aggregation_frames(input_path)
    return dataframes_to_result(daily_df, category_df, product_df)


def build_aggregation_frames(
    input_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """入力CSVから元DataFrameと3つの集計DataFrameを作成する。"""
    # pandas でCSVをDataFrameとして読み込む。
    # Node.js/PHP の「1行ずつパースして配列に詰める」処理に相当。
    df = pd.read_csv(input_path)

    # 明細売上 = quantity * price を新しい列として追加する。
    # 以降のすべての集計は line_total を合計するだけで済むため、
    # 集計ロジックを単純化できる。
    df["line_total"] = df["quantity"] * df["price"]

    # 日次売上:
    # date ごとに line_total を合計し、出力順が安定するよう日付昇順に並べる。
    daily_df = (
        df.groupby("date", as_index=False).agg(sales=("line_total", "sum")).sort_values("date")
    )

    # カテゴリ売上:
    # category ごとに line_total を合計し、売上降順で並べる。
    # 同額の場合の順序ぶれを避けるためカテゴリ名昇順を第二キーにする。
    category_df = (
        df.groupby("category", as_index=False)
        .agg(sales=("line_total", "sum"))
        .sort_values(["sales", "category"], ascending=[False, True])
    )

    # 商品別集計:
    # product ごとに「売上合計(sales)」と「数量合計(quantity)」を同時に作る。
    # 売上降順 + 商品名昇順で並べ、Top3 のみを残す。
    # （Node.js/PHP の sort + slice/array_slice と同じ仕様）
    product_df = (
        df.groupby("product", as_index=False)
        .agg(sales=("line_total", "sum"), quantity=("quantity", "sum"))
        .sort_values(["sales", "product"], ascending=[False, True])
        .head(3)
    )
    return df, daily_df, category_df, product_df


def dataframes_to_result(
    daily_df: pd.DataFrame,
    category_df: pd.DataFrame,
    product_df: pd.DataFrame,
) -> SalesAggregationResult:
    """集計DataFrame群をJSON互換の出力形式へ変換する。"""
    # DataFrame -> JSON互換の list[dict] へ変換する。
    # 各 sales は money() で数値表現を統一する。
    daily_sales: list[DailySalesItem] = [
        {"date": row["date"], "sales": money(row["sales"])} for _, row in daily_df.iterrows()
    ]
    category_sales: list[CategorySalesItem] = [
        {"category": row["category"], "sales": money(row["sales"])}
        for _, row in category_df.iterrows()
    ]
    top_products: list[TopProductItem] = [
        {
            "product": row["product"],
            "sales": money(row["sales"]),
            "quantity": int(row["quantity"]),
        }
        for _, row in product_df.iterrows()
    ]

    return {
        "daily_sales": daily_sales,
        "category_sales": category_sales,
        "top_products": top_products,
    }


def print_debug_dataframes(
    base_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    category_df: pd.DataFrame,
    product_df: pd.DataFrame,
) -> None:
    """JSON出力前のDataFrame状態をログへ出力する。"""
    logger = logging.getLogger(__name__)
    logger.debug("--- 元のDataFrame（line_total追加後）---\n%s", base_df.to_string(index=False))
    logger.debug("--- 日次集計DataFrame（JSON化直前）---\n%s", daily_df.to_string(index=False))
    logger.debug(
        "--- カテゴリ集計DataFrame（JSON化直前）---\n%s",
        category_df.to_string(index=False),
    )
    logger.debug("--- 商品上位DataFrame（JSON化直前）---\n%s", product_df.to_string(index=False))


def setup_logging(debug_dataframe: bool) -> None:
    """実行時オプションに応じてロギングレベルを初期化する。"""
    level = logging.DEBUG if debug_dataframe else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def main() -> None:
    """お題1の売上集計CLIのエントリーポイント。"""
    # CLI引数仕様:
    # --input <csv_path> を必須にし、3言語で同一I/Fに揃える。
    parser = argparse.ArgumentParser(description="売上CSVを集計してJSONで出力します。")
    parser.add_argument("--input", required=True, help="入力CSVのパス")
    parser.add_argument(
        "--debug-dataframe",
        action="store_true",
        help="JSON出力前のDataFrameをDEBUGログとして表示",
    )
    args = parser.parse_args()
    setup_logging(args.debug_dataframe)

    if args.debug_dataframe:
        base_df, daily_df, category_df, product_df = build_aggregation_frames(args.input)
        print_debug_dataframes(base_df, daily_df, category_df, product_df)
        result = dataframes_to_result(daily_df, category_df, product_df)
    else:
        result = aggregate_sales(args.input)

    # 集計を実行し、比較しやすいよう整形JSONで標準出力へ出す。
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    # python -m app.data_processing.sales_aggregation で実行されたときの入口。
    main()
