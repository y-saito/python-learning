const fs = require("fs");

// このファイルは「お題2: JSONログ整形と異常値検出」の Node.js 実装。
// 目的は、Python/PHP と同一I/F・同一出力で比較し、手続き実装の流れを理解すること。
// Node.js では配列操作とループを中心に、処理行程を明示して実装する。

/**
 * CLI引数を解析して入力JSONLパスを取り出す。
 *
 * @param {string[]} argv - プロセスの引数配列。
 * @returns {{ input: string }} 解析済み引数。
 */
function parseArgs(argv) {
  let input = "";
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--input") {
      input = argv[i + 1] || "";
      i += 1;
    }
  }
  if (!input) {
    throw new Error("必須引数がありません: --input <jsonl_path>");
  }
  return { input };
}

/**
 * 数値を比較しやすいJSON表現へ正規化する。
 *
 * @param {number} value - 正規化前の数値。
 * @returns {number} 正規化後の数値。
 */
function normalizeNumber(value) {
  // Python の round + is_integer、PHP の round + 整数化と同じ目的。
  const rounded = Number(value.toFixed(2));
  if (Number.isInteger(rounded)) {
    return rounded;
  }
  return rounded;
}

/**
 * null/空文字を欠損として扱い、数値へ変換する。
 *
 * @param {unknown} raw - 元の値。
 * @returns {number | null} 数値化結果。
 */
function parseNullableNumber(raw) {
  if (raw === null || raw === undefined) {
    return null;
  }
  if (typeof raw === "string" && raw.trim() === "") {
    return null;
  }
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

/**
 * 0.25/0.75分位点を線形補間で計算する（pandas既定に寄せる）。
 *
 * @param {number[]} sortedValues - 昇順にソート済みの配列。
 * @param {number} q - 分位（0.0-1.0）。
 * @returns {number} 分位点。
 */
function quantileLinear(sortedValues, q) {
  const n = sortedValues.length;
  if (n === 0) {
    return NaN;
  }
  if (n === 1) {
    return sortedValues[0];
  }
  const position = (n - 1) * q;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  const lowerValue = sortedValues[lowerIndex];
  const upperValue = sortedValues[upperIndex];
  if (lowerIndex === upperIndex) {
    return lowerValue;
  }
  const weight = position - lowerIndex;
  return lowerValue + (upperValue - lowerValue) * weight;
}

/**
 * JSON Linesを読み込み、整形・補完・異常値/外れ値判定を実行する。
 *
 * 実行順を以下で固定し、Python/PHP と同じ意味になるように揃える。
 * 1) 入力読み込み
 * 2) 型変換
 * 3) 欠損補完
 * 4) 異常値チェック（業務ルール）
 * 5) 外れ値検出（IQR）
 * 6) 出力整形
 *
 * @param {string} input - 入力JSONLパス。
 * @returns {{
 * summary: {
 * total_records: number,
 * filled_response_time_count: number,
 * filled_status_count: number,
 * filled_endpoint_count: number,
 * filled_method_count: number,
 * anomaly_count: number,
 * outlier_count: number
 * },
 * response_time_bounds: { lower: number, upper: number },
 * cleaned_logs: Array<{timestamp: string, endpoint: string, method: string, status: number, response_time_ms: number, is_anomaly: boolean, is_outlier: boolean}>,
 * anomalies: Array<{timestamp: string, endpoint: string, method: string, status: number, response_time_ms: number, is_anomaly: boolean, is_outlier: boolean}>,
 * outliers: Array<{timestamp: string, endpoint: string, method: string, status: number, response_time_ms: number, is_anomaly: boolean, is_outlier: boolean}>
 * }} 結果JSON。
 */
function preprocessLogs(input) {
  // --- 行程1: 入力読み込み（JSONL）---
  // Node.js は file read -> split -> JSON.parse の手順で配列化する。
  // Python なら json_normalize 前提の読み込み、PHP なら file + json_decode が相当。
  const raw = fs.readFileSync(input, "utf8");
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  const rows = lines.map((line) => JSON.parse(line));

  // --- 行程2: 型変換（timestamp / status / response_time_ms）---
  // timestamp は Date へ変換し、無効値は除外する。
  // status/response_time_ms は null/空文字を欠損扱いで数値化する。
  const parsed = rows
    .map((row) => {
      const timestamp = new Date(row.timestamp || "");
      if (Number.isNaN(timestamp.getTime())) {
        return null;
      }

      const statusNumber = parseNullableNumber(row.status);
      const responseNumber = parseNullableNumber(row.response_time_ms);

      const endpointRaw = row.endpoint;
      const methodRaw = row.method;

      return {
        timestamp,
        endpoint:
          endpointRaw === null || endpointRaw === undefined || String(endpointRaw).trim() === ""
            ? null
            : String(endpointRaw),
        method:
          methodRaw === null || methodRaw === undefined || String(methodRaw).trim() === ""
            ? null
            : String(methodRaw),
        status: statusNumber,
        response_time_ms: responseNumber,
      };
    })
    .filter((row) => row !== null);

  // --- 行程3: 欠損補完（補完ルールと件数カウント）---
  // 先に response_time_ms の中央値を求める。
  // ルールは3言語共通: response_time_ms=中央値, status=0, endpoint=/unknown, method=UNKNOWN。
  const validResponseTimes = parsed
    .map((row) => row.response_time_ms)
    .filter((value) => value !== null)
    .sort((a, b) => a - b);

  const mid = Math.floor(validResponseTimes.length / 2);
  const medianResponseTime =
    validResponseTimes.length % 2 === 0
      ? (validResponseTimes[mid - 1] + validResponseTimes[mid]) / 2
      : validResponseTimes[mid];

  let filledResponseTimeCount = 0;
  let filledStatusCount = 0;
  let filledEndpointCount = 0;
  let filledMethodCount = 0;

  for (const row of parsed) {
    if (row.response_time_ms === null) {
      row.response_time_ms = medianResponseTime;
      filledResponseTimeCount += 1;
    }
    if (row.status === null) {
      row.status = 0;
      filledStatusCount += 1;
    }
    if (row.endpoint === null) {
      row.endpoint = "/unknown";
      filledEndpointCount += 1;
    }
    if (row.method === null) {
      row.method = "UNKNOWN";
      filledMethodCount += 1;
    }
  }

  // --- 行程4: 異常値チェック（業務ルール）---
  // 異常値は「論理的にあり得ない値」を扱う。
  // - response_time_ms < 0
  // - status が 0（欠損補完用の許容値）でも 100-599 でもない
  for (const row of parsed) {
    row.is_anomaly = row.response_time_ms < 0 || (row.status !== 0 && (row.status < 100 || row.status > 599));
  }

  // --- 行程5: 外れ値検出（IQR / 境界式）---
  // IQR法:
  // lower = q1 - 1.5 * iqr
  // upper = q3 + 1.5 * iqr
  // Node.js では分位点計算を関数で手実装する。
  // Python は quantile、PHP は同等の quantile_linear 関数を使う。
  const responseTimes = parsed.map((row) => row.response_time_ms).sort((a, b) => a - b);

  const q1 = quantileLinear(responseTimes, 0.25);
  const q3 = quantileLinear(responseTimes, 0.75);
  const iqr = q3 - q1;
  const lowerBound = q1 - 1.5 * iqr;
  const upperBound = q3 + 1.5 * iqr;

  // --- 行程6: 出力整形（並び順・数値正規化・anomalies/outliers抽出）---
  // timestamp 昇順で固定し、比較時に出力順が揺れないようにする。
  parsed.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

  const cleanedLogs = parsed.map((row) => {
    const isOutlier = row.response_time_ms < lowerBound || row.response_time_ms > upperBound;
    return {
      timestamp: row.timestamp.toISOString().replace(".000", ""),
      endpoint: row.endpoint,
      method: row.method,
      status: Math.trunc(row.status),
      response_time_ms: normalizeNumber(row.response_time_ms),
      is_anomaly: row.is_anomaly,
      is_outlier: isOutlier,
    };
  });

  const anomalies = cleanedLogs.filter((item) => item.is_anomaly);
  const outliers = cleanedLogs.filter((item) => item.is_outlier);

  // 返却スキーマ:
  // summary: 件数系の集計情報
  // response_time_bounds: IQR外れ値判定に使った下限/上限
  // cleaned_logs: 全件の整形済みログ
  // anomalies: 業務ルール異常値のみ
  // outliers: IQR外れ値のみ
  return {
    summary: {
      total_records: cleanedLogs.length,
      filled_response_time_count: filledResponseTimeCount,
      filled_status_count: filledStatusCount,
      filled_endpoint_count: filledEndpointCount,
      filled_method_count: filledMethodCount,
      anomaly_count: anomalies.length,
      outlier_count: outliers.length,
    },
    response_time_bounds: {
      lower: normalizeNumber(lowerBound),
      upper: normalizeNumber(upperBound),
    },
    cleaned_logs: cleanedLogs,
    anomalies,
    outliers,
  };
}

/**
 * CLIエントリーポイント。
 * --input の読み取りと結果JSON出力だけを担当する。
 */
function main() {
  const { input } = parseArgs(process.argv);
  const result = preprocessLogs(input);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main();
