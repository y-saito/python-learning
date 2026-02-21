import argparse
import json
import logging
from typing import TypedDict

import pandas as pd
from sqlalchemy import create_engine

# このファイルは「お題4: SQL連携と分析クエリ」の Python 実装。
# Node.js は pg、PHP は PDO を使うのが一般的だが、
# Python は pandas.read_sql_query で抽出後に DataFrame 集計へ直結できる点が強み。


class Summary(TypedDict):
    # SQL抽出後の総件数
    total_rows: int
    # 集計対象期間の開始日
    date_range_start: str
    # 集計対象期間の終了日
    date_range_end: str
    # 全売上合計
    total_revenue: float | int
    # 高額注文件数（しきい値以上）
    high_value_order_count: int


class DailySalesItem(TypedDict):
    # 日付
    date: str
    # 日次売上
    sales: float | int


class SegmentSalesItem(TypedDict):
    # 顧客セグメント名
    segment: str
    # セグメント売上合計
    total_sales: float | int
    # 注文件数
    order_count: int
    # 注文平均売上
    avg_order_amount: float | int


class PaymentMethodSalesItem(TypedDict):
    # 決済手段
    payment_method: str
    # 売上合計
    total_sales: float | int
    # 注文件数
    order_count: int
    # 注文平均売上
    avg_order_amount: float | int


class HighValueOrderItem(TypedDict):
    # 注文ID
    order_id: str
    # 注文日
    order_date: str
    # 顧客セグメント
    segment: str
    # 決済手段
    payment_method: str
    # 注文金額
    order_amount: float | int


class SqlSalesReportResult(TypedDict):
    # 件数・期間・総売上サマリ
    summary: Summary
    # 日次売上
    daily_sales: list[DailySalesItem]
    # セグメント別売上
    segment_sales: list[SegmentSalesItem]
    # 決済手段別売上
    payment_method_sales: list[PaymentMethodSalesItem]
    # 高額注文一覧
    high_value_orders: list[HighValueOrderItem]


def normalize_number(value: float) -> float | int:
    """比較しやすい数値表現へ正規化する（小数第2位、整数は int）。"""
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def setup_logging(debug: bool) -> None:
    """デバッグ有無に応じてロギング設定を初期化する。"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def build_sql_sales_report(
    db_url: str, high_value_threshold: float, debug: bool = False
) -> SqlSalesReportResult:
    """PostgreSQLから売上明細を抽出し、Python側で再集計した結果を返す。"""
    logger = logging.getLogger(__name__)

    # Python:
    #   pandas.read_sql_query で SQL抽出結果を直接 DataFrame 化して分析へ接続する。
    # Node.js:
    #   pg で rows を取り、Map/配列で再集計することが多い。
    # PHP:
    #   PDO で fetchAll 後に foreach 集計することが多い。
    query = """
    SELECT
      order_id,
      order_date::text AS order_date,
      customer_segment,
      payment_method,
      order_amount
    FROM sales_orders
    ORDER BY order_date, order_id
    """
    engine = create_engine(db_url)
    df = pd.read_sql_query(query, con=engine)

    if df.empty:
        raise ValueError("sales_orders にデータがありません。先にシードSQLを投入してください。")

    # 行程1: 型を揃える。SQL抽出時点で文字列/数値が混在しうるため、集計前に明示変換する。
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce")
    if debug:
        logger.debug("--- SQL抽出直後のDataFrame ---\n%s", df.to_string(index=False))

    # 行程2: 集計（DataFrameベース）。
    daily_df = (
        df.groupby("order_date", as_index=False)
        .agg(sales=("order_amount", "sum"))
        .rename(columns={"order_date": "date"})
        .sort_values("date")
    )
    segment_df = (
        df.groupby("customer_segment", as_index=False)
        .agg(
            total_sales=("order_amount", "sum"),
            order_count=("order_id", "count"),
            avg_order_amount=("order_amount", "mean"),
        )
        .rename(columns={"customer_segment": "segment"})
        .sort_values(["total_sales", "segment"], ascending=[False, True])
    )
    payment_df = (
        df.groupby("payment_method", as_index=False)
        .agg(
            total_sales=("order_amount", "sum"),
            order_count=("order_id", "count"),
            avg_order_amount=("order_amount", "mean"),
        )
        .sort_values(["total_sales", "payment_method"], ascending=[False, True])
    )
    high_value_df = df.loc[df["order_amount"] >= high_value_threshold].copy().sort_values(
        ["order_amount", "order_id"], ascending=[False, True]
    )
    if debug:
        logger.debug("--- daily_df ---\n%s", daily_df.to_string(index=False))
        logger.debug("--- segment_df ---\n%s", segment_df.to_string(index=False))
        logger.debug("--- payment_df ---\n%s", payment_df.to_string(index=False))
        logger.debug("--- high_value_df ---\n%s", high_value_df.to_string(index=False))

    # 行程3: DataFrame -> JSON互換オブジェクトへ変換。
    daily_sales: list[DailySalesItem] = [
        {
            # 売上日。
            "date": row["date"].strftime("%Y-%m-%d"),
            # その日の売上合計。
            "sales": normalize_number(row["sales"]),
        }
        for _, row in daily_df.iterrows()
    ]
    segment_sales: list[SegmentSalesItem] = [
        {
            # 顧客セグメント名（Enterprise/SMB/Consumer）。
            "segment": str(row["segment"]),
            # セグメント売上合計。
            "total_sales": normalize_number(row["total_sales"]),
            # セグメント内の注文件数。
            "order_count": int(row["order_count"]),
            # セグメント内の平均注文金額。
            "avg_order_amount": normalize_number(row["avg_order_amount"]),
        }
        for _, row in segment_df.iterrows()
    ]
    payment_method_sales: list[PaymentMethodSalesItem] = [
        {
            # 決済手段名（Card/Invoice など）。
            "payment_method": str(row["payment_method"]),
            # 決済手段ごとの売上合計。
            "total_sales": normalize_number(row["total_sales"]),
            # 決済手段ごとの注文件数。
            "order_count": int(row["order_count"]),
            # 決済手段ごとの平均注文金額。
            "avg_order_amount": normalize_number(row["avg_order_amount"]),
        }
        for _, row in payment_df.iterrows()
    ]
    high_value_orders: list[HighValueOrderItem] = [
        {
            # 注文ID。
            "order_id": str(row["order_id"]),
            # 注文日。
            "order_date": row["order_date"].strftime("%Y-%m-%d"),
            # 注文顧客のセグメント。
            "segment": str(row["customer_segment"]),
            # 注文時の決済手段。
            "payment_method": str(row["payment_method"]),
            # 注文金額（しきい値以上）。
            "order_amount": normalize_number(row["order_amount"]),
        }
        for _, row in high_value_df.iterrows()
    ]

    # 集計サマリを返す。
    min_date = df["order_date"].min()
    max_date = df["order_date"].max()
    if min_date is None or max_date is None:
        raise ValueError("order_date の集計に失敗しました。")
    return {
        # レコード件数・期間・総売上の要約。
        "summary": {
            # SQL抽出後の総件数。
            "total_rows": int(len(df)),
            # 集計期間の開始日。
            "date_range_start": min_date.strftime("%Y-%m-%d"),
            # 集計期間の終了日。
            "date_range_end": max_date.strftime("%Y-%m-%d"),
            # 全注文の売上合計。
            "total_revenue": normalize_number(df["order_amount"].sum()),
            # 高額注文（しきい値以上）の件数。
            "high_value_order_count": int(len(high_value_df)),
        },
        # 日次売上配列。
        "daily_sales": daily_sales,
        # セグメント別売上配列。
        "segment_sales": segment_sales,
        # 決済手段別売上配列。
        "payment_method_sales": payment_method_sales,
        # 高額注文の明細配列。
        "high_value_orders": high_value_orders,
    }


def main() -> None:
    """CLIエントリーポイント。DBから売上を取得して分析結果を出力する。"""
    parser = argparse.ArgumentParser(
        description="PostgreSQLの sales_orders を取得し、Python側で再集計レポートを出力します。"
    )
    parser.add_argument(
        "--db-url",
        default="postgresql+psycopg://app:app@db:5432/app",
        help="SQLAlchemy接続URL。compose内実行時はデフォルト値で接続可能",
    )
    parser.add_argument(
        "--high-value-threshold",
        type=float,
        default=500.0,
        help="高額注文とみなす金額のしきい値",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="途中計算のDataFrameをDEBUGログとして表示",
    )
    args = parser.parse_args()
    setup_logging(args.debug)

    result = build_sql_sales_report(
        db_url=args.db_url,
        high_value_threshold=args.high_value_threshold,
        debug=args.debug,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
