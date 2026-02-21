import argparse
import json
from pathlib import Path
from typing import TypedDict

import matplotlib
import pandas as pd

# コンテナ環境でもGUI不要で描画できるよう、Aggバックエンドを使う。
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# このファイルは「お題5: 可視化によるレポート化」の Python 実装。
# Node.js/PHP では手作業でSVGを組み立てる実装になりやすい一方、
# Python は matplotlib + pandas で「集計 -> 可視化 -> レポート化」を同一言語で完結しやすい。


class Summary(TypedDict):
    # 入力CSVの注文明細数
    total_orders: int
    # 全売上合計
    total_revenue: float | int
    # 注文あたり平均売上
    average_order_value: float | int
    # 売上最大日
    best_sales_day: str
    # 売上最大日の売上金額
    best_sales_amount: float | int


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


class Artifacts(TypedDict):
    # 日次売上グラフのファイル名
    daily_sales_chart: str
    # カテゴリ売上グラフのファイル名
    category_sales_chart: str
    # 上位商品グラフのファイル名
    top_products_chart: str
    # 意思決定向けMarkdownレポートのファイル名
    decision_report_markdown: str


class VisualSalesReportResult(TypedDict):
    # 売上サマリ
    summary: Summary
    # 日次売上
    daily_sales: list[DailySalesItem]
    # カテゴリ売上
    category_sales: list[CategorySalesItem]
    # 売上上位商品
    top_products: list[TopProductItem]
    # 意思決定コメント
    insights: list[str]
    # 生成物パス
    artifacts: Artifacts


def normalize_number(value: float) -> float | int:
    """比較しやすいJSON表現へ正規化する。"""
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def build_aggregations(
    input_path: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """CSVを読み込み、日次・カテゴリ・商品の集計DataFrameを返す。"""
    # Python:
    #   read_csv -> 列演算 -> groupby で集計を宣言的に記述する。
    # Node.js/PHP:
    #   ループで合算Map/連想配列を更新する実装になりやすい。
    df = pd.read_csv(input_path)
    df["line_total"] = df["quantity"] * df["price"]

    daily_df = (
        df.groupby("date", as_index=False).agg(sales=("line_total", "sum")).sort_values("date")
    )
    category_df = (
        df.groupby("category", as_index=False)
        .agg(sales=("line_total", "sum"))
        .sort_values(["sales", "category"], ascending=[False, True])
    )
    product_df = (
        df.groupby("product", as_index=False)
        .agg(sales=("line_total", "sum"), quantity=("quantity", "sum"))
        .sort_values(["sales", "product"], ascending=[False, True])
        .head(3)
    )
    return df, daily_df, category_df, product_df


def save_daily_chart(daily_df: pd.DataFrame, chart_path: Path) -> None:
    """日次売上の折れ線グラフをSVGで保存する。"""
    plt.figure(figsize=(8, 4.5))
    plt.plot(daily_df["date"], daily_df["sales"], marker="o")
    plt.title("Daily Sales Trend")
    plt.xlabel("Date")
    plt.ylabel("Sales")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(chart_path, format="svg")
    plt.close()


def save_category_chart(category_df: pd.DataFrame, chart_path: Path) -> None:
    """カテゴリ売上の棒グラフをSVGで保存する。"""
    plt.figure(figsize=(8, 4.5))
    plt.bar(category_df["category"], category_df["sales"])
    plt.title("Category Sales")
    plt.xlabel("Category")
    plt.ylabel("Sales")
    plt.tight_layout()
    plt.savefig(chart_path, format="svg")
    plt.close()


def save_top_products_chart(product_df: pd.DataFrame, chart_path: Path) -> None:
    """売上上位商品の横棒グラフをSVGで保存する。"""
    plt.figure(figsize=(8, 4.5))
    plt.barh(product_df["product"], product_df["sales"])
    plt.title("Top Products by Sales")
    plt.xlabel("Sales")
    plt.ylabel("Product")
    plt.tight_layout()
    plt.savefig(chart_path, format="svg")
    plt.close()


def build_insights(
    summary: Summary, category_sales: list[CategorySalesItem], top_products: list[TopProductItem]
) -> list[str]:
    """意思決定向けの短い示唆を作成する。"""
    top_category = category_sales[0]["category"]
    top_category_sales = category_sales[0]["sales"]
    top_product = top_products[0]["product"]
    top_product_sales = top_products[0]["sales"]
    best_day = summary["best_sales_day"]
    best_amount = summary["best_sales_amount"]
    return [
        f"売上最大日は {best_day} で、日次売上は {best_amount} です。",
        f"カテゴリ別では {top_category} が最大で、売上は {top_category_sales} です。",
        f"商品別では {top_product} が最大で、売上は {top_product_sales} です。",
    ]


def write_markdown_report(
    report_path: Path,
    summary: Summary,
    insights: list[str],
    artifacts: Artifacts,
) -> None:
    """可視化結果を参照するMarkdownレポートを保存する。"""
    lines = [
        "# お題5 可視化レポート",
        "",
        "## サマリ",
        f"- 総注文件数: {summary['total_orders']}",
        f"- 全売上: {summary['total_revenue']}",
        f"- 注文平均売上: {summary['average_order_value']}",
        f"- 売上最大日: {summary['best_sales_day']} ({summary['best_sales_amount']})",
        "",
        "## 意思決定メモ",
    ]
    for insight in insights:
        lines.append(f"- {insight}")
    lines.extend(
        [
            "",
            "## 生成グラフ",
            f"- 日次売上: `{artifacts['daily_sales_chart']}`",
            f"- カテゴリ売上: `{artifacts['category_sales_chart']}`",
            f"- 上位商品: `{artifacts['top_products_chart']}`",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_visual_sales_report(input_path: str, output_dir: str) -> VisualSalesReportResult:
    """集計・可視化・レポート化を実行し、結果JSONを返す。"""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_df, daily_df, category_df, product_df = build_aggregations(input_path)

    daily_chart = out_dir / "daily_sales.svg"
    category_chart = out_dir / "category_sales.svg"
    product_chart = out_dir / "top_products.svg"
    report_md = out_dir / "decision_report.md"

    save_daily_chart(daily_df, daily_chart)
    save_category_chart(category_df, category_chart)
    save_top_products_chart(product_df, product_chart)

    daily_sales: list[DailySalesItem] = [
        {"date": str(row["date"]), "sales": normalize_number(row["sales"])}
        for _, row in daily_df.iterrows()
    ]
    category_sales: list[CategorySalesItem] = [
        {"category": str(row["category"]), "sales": normalize_number(row["sales"])}
        for _, row in category_df.iterrows()
    ]
    top_products: list[TopProductItem] = [
        {
            "product": str(row["product"]),
            "sales": normalize_number(row["sales"]),
            "quantity": int(row["quantity"]),
        }
        for _, row in product_df.iterrows()
    ]

    best_day = daily_sales[0]
    for item in daily_sales:
        if float(item["sales"]) > float(best_day["sales"]):
            best_day = item
    total_revenue = sum(float(item["sales"]) for item in daily_sales)
    summary: Summary = {
        # 入力CSVの注文明細数。
        "total_orders": int(len(base_df)),
        # 全売上合計。
        "total_revenue": normalize_number(total_revenue),
        # 注文1件あたり平均売上。
        "average_order_value": normalize_number(total_revenue / len(base_df)),
        # 売上最大日。
        "best_sales_day": best_day["date"],
        # 売上最大日の売上。
        "best_sales_amount": best_day["sales"],
    }

    artifacts: Artifacts = {
        # 日次売上グラフファイル名。
        "daily_sales_chart": daily_chart.name,
        # カテゴリ売上グラフファイル名。
        "category_sales_chart": category_chart.name,
        # 上位商品グラフファイル名。
        "top_products_chart": product_chart.name,
        # 意思決定レポートファイル名。
        "decision_report_markdown": report_md.name,
    }
    insights = build_insights(summary, category_sales, top_products)
    write_markdown_report(report_md, summary, insights, artifacts)

    return {
        # 売上全体の主要指標。
        "summary": summary,
        # 日単位の売上推移。
        "daily_sales": daily_sales,
        # カテゴリ単位の売上。
        "category_sales": category_sales,
        # 売上上位商品の明細。
        "top_products": top_products,
        # 意思決定のための短い示唆。
        "insights": insights,
        # 生成したグラフ/レポートの保存先。
        "artifacts": artifacts,
    }


def main() -> None:
    """CLIエントリーポイント。"""
    parser = argparse.ArgumentParser(
        description="売上CSVを可視化し、意思決定向けレポートを生成します。"
    )
    parser.add_argument("--input", required=True, help="入力CSVのパス")
    parser.add_argument("--output-dir", required=True, help="可視化出力ディレクトリ")
    args = parser.parse_args()

    result = build_visual_sales_report(args.input, args.output_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
