import argparse
import json
import logging
from typing import TypedDict

import pandas as pd

# このファイルは「お題2: JSONログ整形と異常値検出」の Python 実装。
# 目的は、Node.js / PHP 実装と同じ入力・同じ出力スキーマで比較学習できること。
# Python側は pandas を使い、前処理を「列単位で宣言的に書ける」点を示す。
# そのため、各行程（読み込み -> 型変換 -> 補完 -> 異常値チェック -> 外れ値検出 -> 出力整形）を
# 明示する。


# 以下の TypedDict 群は「出力JSONの型付き仕様書」。
# Node.js なら TypeScript の interface、PHP なら配列仕様ドキュメントに相当する。
class CleanedLogItem(TypedDict):
    # 正規化済みタイムスタンプ（UTC / ISO8601）
    timestamp: str
    # エンドポイント（欠損時は /unknown）
    endpoint: str
    # HTTPメソッド（欠損時は UNKNOWN）
    method: str
    # HTTPステータス（欠損時は 0）
    status: int
    # レスポンス時間（ms、欠損時は中央値で補完）
    response_time_ms: float | int
    # 業務ルール判定による異常値フラグ
    is_anomaly: bool
    # IQR判定による外れ値フラグ
    is_outlier: bool


class Summary(TypedDict):
    # 処理対象レコード総数
    total_records: int
    # response_time_ms を補完した件数
    filled_response_time_count: int
    # status を補完した件数
    filled_status_count: int
    # endpoint を補完した件数
    filled_endpoint_count: int
    # method を補完した件数
    filled_method_count: int
    # 異常値件数（業務ルール）
    anomaly_count: int
    # 外れ値件数
    outlier_count: int


class ResponseTimeBounds(TypedDict):
    # IQR法で算出した下限
    lower: float | int
    # IQR法で算出した上限
    upper: float | int


class LogPreprocessResult(TypedDict):
    # 件数サマリ
    summary: Summary
    # 外れ値判定境界
    response_time_bounds: ResponseTimeBounds
    # 整形済みログ本体
    cleaned_logs: list[CleanedLogItem]
    # 異常値のみ抽出した配列
    anomalies: list[CleanedLogItem]
    # 外れ値のみ抽出した配列
    outliers: list[CleanedLogItem]


def normalize_number(value: float) -> float | int:
    """比較用JSONで数値表現を統一する。

    Node.js では toFixed + Number、PHP では round で同等目的を達成する。
    Python では round 後に整数判定し、`25.0` ではなく `25` へ寄せる。
    """
    rounded = round(float(value), 2)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def parse_jsonl(input_path: str) -> list[dict[str, object]]:
    """JSON Lines を1行ずつパースしてオブジェクト配列へ変換する。

    Args:
        input_path: 入力JSONLのパス。

    Returns:
        行ごとの dict を格納した配列。

    Raises:
        ValueError: JSONLの行がオブジェクトでない場合。
    """
    rows: list[dict[str, object]] = []
    with open(input_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            obj = json.loads(stripped)
            if not isinstance(obj, dict):
                raise ValueError("JSON Lines の各行はオブジェクトである必要があります")
            rows.append(obj)
    return rows


def preprocess_logs(input_path: str) -> tuple[LogPreprocessResult, pd.DataFrame]:
    """JSONログを整形し、IQRで外れ値を判定して結果JSONを組み立てる。

    処理順序は以下で固定する。
    1. 入力読み込み（JSONL）
    2. 型変換
    3. 欠損補完
    4. 異常値チェック（業務ルール）
    5. 外れ値検出（IQR）
    6. 出力整形

    この順序にする理由は、型変換失敗を欠損に落としてから補完することで、
    各言語で同じ判定条件を保ちやすくするため。
    """
    # --- 行程1: 入力読み込み（JSONL）---
    # Node.js/PHP では行ループで配列へ格納するが、Python はこの後すぐ DataFrame 化する。
    records = parse_jsonl(input_path)
    df = pd.json_normalize(records)

    # JSON行ごとにキーが揃わないケースに備えて必要列を先に定義する。
    # Node.js/PHP ではキー存在チェックを各行で行うが、Python+pandas は列単位で補完できる。
    required_columns = ["timestamp", "endpoint", "method", "status", "response_time_ms"]
    for col in required_columns:
        if col not in df.columns:
            df[col] = pd.NA

    # --- 行程2: 型変換（timestamp / status / response_time_ms）---
    # timestamp: 文字列 -> UTC datetime
    # status/response_time_ms: 文字列数値を含めて数値化。失敗時は NaN にして後段で補完する。
    # Node.js/PHP だと Number/is_numeric 判定を行ループ内で実施する処理に相当。
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["status"] = pd.to_numeric(df["status"], errors="coerce")
    df["response_time_ms"] = pd.to_numeric(df["response_time_ms"], errors="coerce")

    # timestamp が無効な行は学習上のノイズを避けるため除外する。
    df = df.dropna(subset=["timestamp"]).copy()

    # --- 行程3: 欠損補完（補完ルールと件数カウント）---
    # ルール:
    # - response_time_ms: 中央値
    # - status: 0
    # - endpoint: /unknown
    # - method: UNKNOWN
    # Node.js/PHP はループで if 判定しながら補完する。
    # Python はマスクを作り、補完件数カウントと値更新を列単位で一括実行する。
    median_response_time = float(df["response_time_ms"].median())
    response_missing_mask = df["response_time_ms"].isna()
    status_missing_mask = df["status"].isna()
    endpoint_missing_mask = df["endpoint"].isna() | (df["endpoint"].astype(str).str.strip() == "")
    method_missing_mask = df["method"].isna() | (df["method"].astype(str).str.strip() == "")

    filled_response_time_count = int(response_missing_mask.sum())
    filled_status_count = int(status_missing_mask.sum())
    filled_endpoint_count = int(endpoint_missing_mask.sum())
    filled_method_count = int(method_missing_mask.sum())

    df.loc[response_missing_mask, "response_time_ms"] = median_response_time
    df.loc[status_missing_mask, "status"] = 0
    df.loc[endpoint_missing_mask, "endpoint"] = "/unknown"
    df.loc[method_missing_mask, "method"] = "UNKNOWN"

    # --- 行程4: 異常値チェック（業務ルール）---
    # 異常値と外れ値は別概念として分離する。
    # - 異常値: 論理的にあり得ない値（例: response_time_ms < 0、status範囲外）
    # - 外れ値: 分布上で極端に離れた値（IQRで判定）
    # Node.js/PHP でも同じルールで判定する。
    df["is_anomaly"] = (df["response_time_ms"] < 0) | (
        (df["status"] != 0) & ((df["status"] < 100) | (df["status"] > 599))
    )

    # --- 行程5: 外れ値検出（IQR / 境界式）---
    # IQR法:
    #   q1 = 25%点, q3 = 75%点, iqr = q3 - q1
    #   lower = q1 - 1.5 * iqr
    #   upper = q3 + 1.5 * iqr
    # Node.js/PHP では分位点計算を関数実装しているが、Python は quantile で直接求められる。
    q1 = float(df["response_time_ms"].quantile(0.25))
    q3 = float(df["response_time_ms"].quantile(0.75))
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    df["is_outlier"] = (df["response_time_ms"] < lower_bound) | (
        df["response_time_ms"] > upper_bound
    )

    # --- 行程6: 出力整形（並び順・数値正規化・anomalies/outliers抽出）---
    # まず timestamp 昇順に固定して、3言語で出力順の揺れをなくす。
    df = df.sort_values("timestamp").copy()

    # JSON互換形式に寄せるため、列型を最終形へそろえる。
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["status"] = df["status"].astype(int)
    df["endpoint"] = df["endpoint"].astype(str)
    df["method"] = df["method"].astype(str)

    cleaned_logs: list[CleanedLogItem] = []
    for _, row in df.iterrows():
        # 1行ずつ辞書へ落とし、数値表現を normalize_number で統一する。
        cleaned_logs.append(
            {
                "timestamp": str(row["timestamp"]),
                "endpoint": str(row["endpoint"]),
                "method": str(row["method"]),
                "status": int(row["status"]),
                "response_time_ms": normalize_number(row["response_time_ms"]),
                "is_anomaly": bool(row["is_anomaly"]),
                "is_outlier": bool(row["is_outlier"]),
            }
        )

    anomalies = [item for item in cleaned_logs if item["is_anomaly"]]
    outliers = [item for item in cleaned_logs if item["is_outlier"]]

    # 返却スキーマ:
    # summary: 件数系の集計情報
    # response_time_bounds: IQR外れ値判定に使った下限/上限
    # cleaned_logs: 全件の整形済みログ
    # anomalies: 業務ルール異常値のみ
    # outliers: IQR外れ値のみ
    result: LogPreprocessResult = {
        "summary": {
            "total_records": len(cleaned_logs),
            "filled_response_time_count": filled_response_time_count,
            "filled_status_count": filled_status_count,
            "filled_endpoint_count": filled_endpoint_count,
            "filled_method_count": filled_method_count,
            "anomaly_count": len(anomalies),
            "outlier_count": len(outliers),
        },
        "response_time_bounds": {
            "lower": normalize_number(lower_bound),
            "upper": normalize_number(upper_bound),
        },
        "cleaned_logs": cleaned_logs,
        "anomalies": anomalies,
        "outliers": outliers,
    }
    return result, df


def print_debug_dataframe(df: pd.DataFrame) -> None:
    """JSON化直前のDataFrameをデバッグ表示する。"""
    logger = logging.getLogger(__name__)
    logger.debug("--- 整形後DataFrame（JSON化直前）---\n%s", df.to_string(index=False))


def setup_logging(debug_dataframe: bool) -> None:
    """--debug-dataframe の有無に応じてログレベルを初期化する。"""
    level = logging.DEBUG if debug_dataframe else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def main() -> None:
    """CLIエントリーポイント。--input を受け取り整形結果JSONを出力する。"""
    parser = argparse.ArgumentParser(
        description="JSONログを整形し、外れ値を検出してJSONで出力します。"
    )
    parser.add_argument("--input", required=True, help="入力JSON Linesファイルのパス")
    parser.add_argument(
        "--debug-dataframe",
        action="store_true",
        help="JSON出力前のDataFrameをDEBUGログとして表示",
    )
    args = parser.parse_args()

    setup_logging(args.debug_dataframe)
    result, df = preprocess_logs(args.input)

    if args.debug_dataframe:
        print_debug_dataframe(df)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
