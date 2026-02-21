import argparse
import json
import logging
from typing import TypedDict, cast

import pandas as pd

# このファイルは「お題3: 複数データの結合（JOIN相当）」の Python 実装。
# Node.js/PHP が Map や連想配列で突合するのに対し、
# Python は pandas.merge で INNER/LEFT JOIN を宣言的に記述できる点を学ぶ。


class Summary(TypedDict):
    # 顧客マスタ件数
    customers_count: int
    # 注文データ件数
    orders_count: int
    # INNER JOIN 後の件数
    inner_join_rows: int
    # LEFT JOIN 後の件数
    left_join_rows: int
    # 顧客マスタに存在しない注文件数
    orphan_order_count: int


class SegmentSalesItem(TypedDict):
    # セグメント名（Enterprise, SMB, Consumer, Unknown）
    segment: str
    # セグメント合計売上
    total_sales: float | int
    # 注文件数
    order_count: int
    # 注文平均売上
    avg_order_amount: float | int
    # ユニーク顧客数
    unique_customers: int


class TopProductBySegmentItem(TypedDict):
    # セグメント名
    segment: str
    # セグメント内の売上トップ商品
    product: str
    # トップ商品の売上合計
    total_sales: float | int
    # トップ商品の数量合計
    total_quantity: int


class OrphanOrderItem(TypedDict):
    # 注文ID
    order_id: str
    # 注文日
    order_date: str
    # 顧客ID
    customer_id: str
    # 商品名
    product: str
    # 明細売上（quantity * unit_price）
    line_total: float | int


class CustomerOrderJoinResult(TypedDict):
    # 件数系サマリ
    summary: Summary
    # INNER JOIN ベースのセグメント売上
    segment_sales_inner: list[SegmentSalesItem]
    # LEFT JOIN ベースのセグメント売上（Unknown 含む）
    segment_sales_left: list[SegmentSalesItem]
    # INNER JOIN ベースのセグメント別トップ商品
    top_products_by_segment_inner: list[TopProductBySegmentItem]
    # マスタ未登録顧客の注文一覧
    orphan_orders: list[OrphanOrderItem]


def normalize_number(value: float) -> float | int:
    """比較しやすい数値表現へ正規化する（小数第2位、整数は int）。"""
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def build_segment_sales(df: pd.DataFrame) -> pd.DataFrame:
    """セグメント単位の売上サマリDataFrameを作る。

    Python では groupby/agg で集計軸を宣言的にまとめられる。
    Node.js/PHP では同じ処理をループ内の加算で表現する実装が一般的。
    """
    grouped = df.groupby("segment", as_index=False).agg(
        total_sales=("line_total", "sum"),
        order_count=("order_id", "count"),
        avg_order_amount=("line_total", "mean"),
        unique_customers=("customer_id", "nunique"),
    )
    return grouped.sort_values(["total_sales", "segment"], ascending=[False, True])


def setup_logging(debug: bool) -> None:
    """デバッグ有無に応じてロギング設定を初期化する。"""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def join_and_analyze(
    customers_path: str, orders_path: str, debug: bool = False
) -> CustomerOrderJoinResult:
    """顧客マスタと注文データを INNER/LEFT JOIN し、セグメント分析結果を返す。"""
    logger = logging.getLogger(__name__)
    # Python:
    #   pandas.read_csv + merge でJOIN処理を宣言的に書く。
    # Node.js/PHP:
    #   Map/連想配列を構築し、注文ループで突合する実装になりやすい。
    customers_df = pd.read_csv(customers_path)
    orders_df = pd.read_csv(orders_path)
    if debug:
        logger.debug("--- customers_df 読み込み直後 ---\n%s", customers_df.to_string(index=False))
        logger.debug("--- orders_df 読み込み直後 ---\n%s", orders_df.to_string(index=False))

    # 行程1: 集計共通列の作成。
    # line_total を事前作成しておくと、INNER/LEFT のどちらでも同じ式を再利用できる。
    # Node.js/PHP ではループ処理の中で同等の値を都度作ることが多い。
    orders_df["line_total"] = orders_df["quantity"] * orders_df["unit_price"]
    if debug:
        logger.debug(
            "--- orders_df line_total 追加後 ---\n%s",
            orders_df.to_string(index=False),
        )

    # INNER JOIN: 顧客マスタに存在する注文のみ分析対象にする。
    inner_df = orders_df.merge(customers_df, on="customer_id", how="inner")
    if debug:
        logger.debug("--- inner_df (INNER JOIN 後) ---\n%s", inner_df.to_string(index=False))

    # LEFT JOIN: 注文を基準に全件残し、未突合は Unknown として扱う。
    # orphan(顧客未登録注文)をこの時点で抽出しておくと、
    # 後続の集計ロジックと監査ロジックを分離できる。
    left_df = orders_df.merge(customers_df, on="customer_id", how="left")
    # pandas-stubs では loc の戻り値が DataFrame | Series と推論される場合がある。
    # 実際のこの式は行フィルタなので DataFrame となるため、cast で型を確定する。
    orphan_df = cast(  # type: ignore[redundant-cast]
        pd.DataFrame, left_df.loc[left_df["customer_name"].isna()].copy()
    )
    left_df["segment"] = left_df["segment"].fillna("Unknown")
    if debug:
        logger.debug("--- left_df (LEFT JOIN 後) ---\n%s", left_df.to_string(index=False))
        logger.debug("--- orphan_df (顧客未登録注文) ---\n%s", orphan_df.to_string(index=False))

    # 行程2: セグメント売上集計。
    # INNER は「マスタに存在する注文だけの実績」、
    # LEFT は「未登録顧客注文を Unknown として含めた全体実績」を表す。
    segment_inner_df = build_segment_sales(inner_df)
    segment_left_df = build_segment_sales(left_df)
    if debug:
        logger.debug("--- segment_inner_df ---\n%s", segment_inner_df.to_string(index=False))
        logger.debug("--- segment_left_df ---\n%s", segment_left_df.to_string(index=False))

    # セグメント x 商品 で売上を作り、各セグメントのトップ商品を1件ずつ抽出する。
    top_product_df = (
        inner_df.groupby(["segment", "product"], as_index=False)
        .agg(total_sales=("line_total", "sum"), total_quantity=("quantity", "sum"))
        .sort_values(["segment", "total_sales", "product"], ascending=[True, False, True])
        .drop_duplicates(subset=["segment"], keep="first")
        .sort_values(["total_sales", "segment"], ascending=[False, True])
    )
    if debug:
        logger.debug(
            "--- top_product_df (セグメント別トップ商品抽出前段) ---\n%s",
            top_product_df.to_string(index=False),
        )

    # 行程3: DataFrame -> JSON互換dict配列へ変換。
    # 型を明示しておくと、mypy でキー/型の崩れを検知できる。
    segment_sales_inner: list[SegmentSalesItem] = [
        {
            "segment": str(row["segment"]),
            "total_sales": normalize_number(row["total_sales"]),
            "order_count": int(row["order_count"]),
            "avg_order_amount": normalize_number(row["avg_order_amount"]),
            "unique_customers": int(row["unique_customers"]),
        }
        for _, row in segment_inner_df.iterrows()
    ]

    segment_sales_left: list[SegmentSalesItem] = [
        {
            "segment": str(row["segment"]),
            "total_sales": normalize_number(row["total_sales"]),
            "order_count": int(row["order_count"]),
            "avg_order_amount": normalize_number(row["avg_order_amount"]),
            "unique_customers": int(row["unique_customers"]),
        }
        for _, row in segment_left_df.iterrows()
    ]

    top_products_by_segment_inner: list[TopProductBySegmentItem] = [
        {
            "segment": str(row["segment"]),
            "product": str(row["product"]),
            "total_sales": normalize_number(row["total_sales"]),
            "total_quantity": int(row["total_quantity"]),
        }
        for _, row in top_product_df.iterrows()
    ]

    # 行程4: orphan_orders は order_date -> order_id で固定順にする。
    # 比較学習で diff 検証しやすくするため、出力順を安定化する。
    orphan_orders: list[OrphanOrderItem] = [
        {
            "order_id": str(row["order_id"]),
            "order_date": str(row["order_date"]),
            "customer_id": str(row["customer_id"]),
            "product": str(row["product"]),
            "line_total": normalize_number(row["line_total"]),
        }
        for _, row in orphan_df.sort_values(by=["order_date", "order_id"]).iterrows()
    ]

    return {
        # 集計処理全体の件数サマリ。
        "summary": {
            # 顧客マスタCSVの総件数。
            "customers_count": int(len(customers_df)),
            # 注文CSVの総件数。
            "orders_count": int(len(orders_df)),
            # INNER JOINで突合できた注文件数。
            "inner_join_rows": int(len(inner_df)),
            # LEFT JOINで保持した注文件数（orders総件数と同値）。
            "left_join_rows": int(len(left_df)),
            # 顧客マスタ未登録の注文件数。
            "orphan_order_count": int(len(orphan_df)),
        },
        # INNER JOINベースのセグメント売上集計。
        "segment_sales_inner": segment_sales_inner,
        # LEFT JOINベースのセグメント売上集計（Unknown含む）。
        "segment_sales_left": segment_sales_left,
        # INNER JOINデータから算出したセグメント別トップ商品。
        "top_products_by_segment_inner": top_products_by_segment_inner,
        # 顧客マスタ未登録注文の明細一覧。
        "orphan_orders": orphan_orders,
    }


def main() -> None:
    """CLIエントリーポイント。顧客CSVと注文CSVを受けてJOIN分析結果を出力する。"""
    parser = argparse.ArgumentParser(
        description="顧客CSVと注文CSVをJOINしてセグメント別の購入傾向を出力します。"
    )
    parser.add_argument("--customers", required=True, help="顧客マスタCSVのパス")
    parser.add_argument("--orders", required=True, help="注文CSVのパス")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="途中計算のDataFrameをDEBUGログとして表示",
    )
    args = parser.parse_args()
    setup_logging(args.debug)

    result = join_and_analyze(args.customers, args.orders, debug=args.debug)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
