const fs = require("fs");
const parquet = require("parquetjs-lite");

// このファイルは「お題7: ETLミニパイプライン」の Node.js 実装。
// Node.js では行ループ + 手続き的変換で ETL を書くことが多い。
// Python(pandas) と同じETL段階/同じ出力スキーマに揃えて比較する。

/**
 * CLI引数を解析する。
 *
 * @param {string[]} argv CLI引数。
 * @returns {{input: string, output: string, debug: boolean}} 実行オプション。
 */
function parseArgs(argv) {
  let input = "";
  let output = "";
  let debug = false;

  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--input") {
      input = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--output") {
      output = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--debug") {
      debug = true;
    }
  }

  if (!input) {
    throw new Error("必須引数がありません: --input <csv_path>");
  }
  if (!output) {
    throw new Error("必須引数がありません: --output <parquet_path>");
  }

  return { input, output, debug };
}

/**
 * 比較しやすい数値表現へ正規化する。
 *
 * @param {number} value 正規化前の値。
 * @returns {number} 正規化後の値。
 */
function normalizeNumber(value) {
  const rounded = Number(value.toFixed(2));
  if (Number.isInteger(rounded)) {
    return rounded;
  }
  return rounded;
}

/**
 * ヘッダー付きCSVを最小構成で読み込む。
 *
 * @param {string} path CSVパス。
 * @returns {Array<Record<string, string>>} 行配列。
 */
function readCsv(path) {
  const raw = fs.readFileSync(path, "utf8").trim();
  const lines = raw.split(/\r?\n/);
  const headers = lines[0].split(",");

  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || "";
    });
    return row;
  });
}

/**
 * 数値文字列をパースする。空文字は NaN として扱う。
 *
 * @param {string} value 入力文字列。
 * @returns {number} 数値。無効時は NaN。
 */
function parseOptionalNumber(value) {
  const trimmed = String(value).trim();
  if (trimmed === "") {
    return Number.NaN;
  }
  return Number(trimmed);
}

/**
 * Date parse成否を判定し、成功時は YYYY-MM-DD を返す。
 *
 * @param {string} value 日付文字列。
 * @returns {string | null} 正規化日付。
 */
function normalizeDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  const iso = date.toISOString();
  return iso.slice(0, 10);
}

/**
 * 配列中央値を返す。
 *
 * @param {number[]} values 数値配列。
 * @param {number} fallback 空配列時の代替値。
 * @returns {number} 中央値。
 */
function median(values, fallback) {
  if (values.length === 0) {
    return fallback;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }
  return sorted[mid];
}

/**
 * Transform: 欠損補完・型変換・派生列追加を行う。
 *
 * @param {Array<Record<string, string>>} rawRows 生注文行。
 * @returns {{cleanedRows: Array<object>, transformStats: object}} 変換結果。
 */
function transformOrders(rawRows) {
  // 行程1: trim + 日付正規化。Node.js/PHP は行ごとの判定処理で書くことが多い。
  // Python は pandas.to_datetime + mask 演算で列単位に処理できる。
  const withDate = rawRows.map((row) => {
    const orderId = String(row.order_id || "").trim();
    const orderDateRaw = String(row.order_date || "").trim();
    const customerId = String(row.customer_id || "").trim();
    const product = String(row.product || "").trim();
    const quantityRaw = String(row.quantity || "").trim();
    const unitPriceRaw = String(row.unit_price || "").trim();

    return {
      order_id: orderId,
      order_date: normalizeDate(orderDateRaw),
      customer_id: customerId,
      product,
      quantity_raw: quantityRaw,
      unit_price_raw: unitPriceRaw,
    };
  });

  const droppedInvalidOrderDateCount = withDate.filter((row) => row.order_date === null).length;
  const validDateRows = withDate.filter((row) => row.order_date !== null);

  // 行程2: 数値変換 + 補完値算出。
  const quantityCandidates = [];
  const unitPriceCandidates = [];
  for (const row of validDateRows) {
    const quantityNum = parseOptionalNumber(row.quantity_raw);
    const unitPriceNum = parseOptionalNumber(row.unit_price_raw);
    if (Number.isFinite(quantityNum) && quantityNum > 0) {
      quantityCandidates.push(quantityNum);
    }
    if (Number.isFinite(unitPriceNum) && unitPriceNum >= 0) {
      unitPriceCandidates.push(unitPriceNum);
    }
  }

  const quantityFillValue = median(quantityCandidates, 1);
  const unitPriceFillValue = median(unitPriceCandidates, 0);

  // 行程3: 補完と派生列作成。
  let filledCustomerIdCount = 0;
  let filledQuantityCount = 0;
  let filledUnitPriceCount = 0;

  const cleanedRows = validDateRows
    .map((row) => {
      let customerId = row.customer_id;
      if (!customerId) {
        customerId = "UNKNOWN_CUSTOMER";
        filledCustomerIdCount += 1;
      }

      let quantity = parseOptionalNumber(row.quantity_raw);
      if (!Number.isFinite(quantity) || quantity <= 0) {
        quantity = quantityFillValue;
        filledQuantityCount += 1;
      }

      let unitPrice = parseOptionalNumber(row.unit_price_raw);
      if (!Number.isFinite(unitPrice) || unitPrice < 0) {
        unitPrice = unitPriceFillValue;
        filledUnitPriceCount += 1;
      }

      const orderDate = String(row.order_date);
      return {
        order_id: row.order_id,
        order_date: orderDate,
        customer_id: customerId,
        product: row.product,
        quantity: Math.round(quantity),
        unit_price: Number(unitPrice),
        order_month: orderDate.slice(0, 7),
        line_total: normalizeNumber(Math.round(quantity) * Number(unitPrice)),
      };
    })
    .sort((a, b) => a.order_date.localeCompare(b.order_date) || a.order_id.localeCompare(b.order_id));

  return {
    cleanedRows,
    transformStats: {
      transformed_records: cleanedRows.length,
      dropped_invalid_order_date_count: droppedInvalidOrderDateCount,
      filled_customer_id_count: filledCustomerIdCount,
      filled_quantity_count: filledQuantityCount,
      filled_unit_price_count: filledUnitPriceCount,
      quantity_fill_value: normalizeNumber(quantityFillValue),
      unit_price_fill_value: normalizeNumber(unitPriceFillValue),
    },
  };
}

/**
 * Load: Parquetファイルへ保存する。
 *
 * @param {Array<object>} cleanedRows 整形済み行。
 * @param {string} outputPath 出力Parquetパス。
 * @returns {Promise<{output_path: string, loaded_records: number}>} 保存統計。
 */
async function loadParquet(cleanedRows, outputPath) {
  // Node.js では parquetjs-lite でスキーマ定義が必要。
  // Python の DataFrame.to_parquet と比較すると、Load工程の記述量が増えやすい。
  const schema = new parquet.ParquetSchema({
    order_id: { type: "UTF8" },
    order_date: { type: "UTF8" },
    customer_id: { type: "UTF8" },
    product: { type: "UTF8" },
    quantity: { type: "INT64" },
    unit_price: { type: "DOUBLE" },
    order_month: { type: "UTF8" },
    line_total: { type: "DOUBLE" },
  });

  fs.mkdirSync(require("path").dirname(outputPath), { recursive: true });
  const writer = await parquet.ParquetWriter.openFile(schema, outputPath);
  for (const row of cleanedRows) {
    await writer.appendRow({
      ...row,
      line_total: Number(row.line_total),
      unit_price: Number(row.unit_price),
    });
  }
  await writer.close();

  const normalizedOutputPath = outputPath.startsWith("/workspace/")
    ? outputPath.slice("/workspace/".length)
    : outputPath;

  return {
    output_path: normalizedOutputPath,
    loaded_records: cleanedRows.length,
  };
}

/**
 * CLIエントリーポイント。
 *
 * @returns {Promise<void>}
 */
async function main() {
  const { input, output, debug } = parseArgs(process.argv);

  // Extract: 入力CSVを読んで件数を確定。
  const rawRows = readCsv(input);
  const extractStats = { input_records: rawRows.length };

  const { cleanedRows, transformStats } = transformOrders(rawRows);
  const loadStats = await loadParquet(cleanedRows, output);

  if (debug) {
    process.stderr.write(`[debug] rawRows=${JSON.stringify(rawRows, null, 2)}\n`);
    process.stderr.write(`[debug] cleanedRows=${JSON.stringify(cleanedRows, null, 2)}\n`);
  }

  const totalSales = cleanedRows.reduce((sum, row) => sum + Number(row.line_total), 0);

  const result = {
    summary: {
      extract: extractStats,
      transform: transformStats,
      load: loadStats,
      total_sales: normalizeNumber(totalSales),
    },
    sample_cleaned_rows: cleanedRows.slice(0, 3).map((row) => ({
      ...row,
      unit_price: normalizeNumber(Number(row.unit_price)),
      line_total: normalizeNumber(Number(row.line_total)),
    })),
  };

  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  const message =
    error && error.stack ? error.stack : error && error.message ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
