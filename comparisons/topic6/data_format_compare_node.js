const fs = require("fs");
const parquet = require("parquetjs-lite");

// このファイルは「お題6: JSON / Parquet 読み込み比較」の Node.js 実装。
// Node.js では JSON は標準で扱いやすい一方、Parquet は parquetjs-lite 導入が必要。
// Python(pandas.read_json/read_parquet)との実装差を比較することが目的。

/**
 * CLI引数を解析する。
 *
 * @param {string[]} argv - CLI引数。
 * @returns {{ jsonInput: string, parquetInput: string, debug: boolean }} 実行オプション。
 */
function parseArgs(argv) {
  let jsonInput = "";
  let parquetInput = "";
  let debug = false;

  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--json-input") {
      jsonInput = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--parquet-input") {
      parquetInput = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--debug") {
      debug = true;
    }
  }

  if (!jsonInput) {
    throw new Error("必須引数がありません: --json-input <path>");
  }
  if (!parquetInput) {
    throw new Error("必須引数がありません: --parquet-input <path>");
  }

  return { jsonInput, parquetInput, debug };
}

/**
 * 数値を比較しやすいJSON表現へ正規化する。
 *
 * @param {number} value - 正規化前の値。
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
 * JSONファイルを読み込み、売上明細配列として返す。
 *
 * @param {string} inputPath - JSONファイルパス。
 * @returns {Array<{date: string, product: string, category: string, quantity: number, price: number}>} 売上明細。
 */
function readJsonSales(inputPath) {
  // Node.js: JSONは標準のJSON.parseだけで読める。
  // PHPでも json_decode が同じ役割。Pythonは pandas.read_json でDataFrameへ直接取り込める。
  const raw = fs.readFileSync(inputPath, "utf8");
  const rows = JSON.parse(raw);
  return rows.map((row) => ({
    date: String(row.date),
    product: String(row.product),
    category: String(row.category),
    quantity: Number(row.quantity),
    price: Number(row.price),
  }));
}

/**
 * Parquetファイルを読み込み、売上明細配列として返す。
 *
 * @param {string} inputPath - Parquetファイルパス。
 * @returns {Promise<Array<{date: string, product: string, category: string, quantity: number, price: number}>>} 売上明細。
 */
async function readParquetSales(inputPath) {
  // Node.js: Parquetは追加ライブラリ(parquetjs-lite)が必要。
  const reader = await parquet.ParquetReader.openFile(inputPath);
  const cursor = reader.getCursor();

  const rows = [];
  let record = await cursor.next();
  while (record) {
    rows.push({
      date: String(record.date),
      product: String(record.product),
      category: String(record.category),
      quantity: Number(record.quantity),
      price: Number(record.price),
    });
    record = await cursor.next();
  }

  await reader.close();
  return rows;
}

/**
 * 売上明細配列を日次・カテゴリ・商品Top3で集計する。
 *
 * @param {Array<{date: string, product: string, category: string, quantity: number, price: number}>} rows - 売上明細。
 * @returns {{daily_sales: Array<{date: string, sales: number}>, category_sales: Array<{category: string, sales: number}>, top_products: Array<{product: string, sales: number, quantity: number}>}} 集計結果。
 */
function aggregateSales(rows) {
  // Node.js/PHP は Map/連想配列を更新して集計を手続き的に作る。
  // Python は groupby/agg を宣言的に記述できる点が比較ポイント。
  const dailyMap = new Map();
  const categoryMap = new Map();
  const productMap = new Map();

  for (const row of rows) {
    const lineTotal = row.quantity * row.price;
    dailyMap.set(row.date, (dailyMap.get(row.date) || 0) + lineTotal);
    categoryMap.set(row.category, (categoryMap.get(row.category) || 0) + lineTotal);

    if (!productMap.has(row.product)) {
      productMap.set(row.product, { sales: 0, quantity: 0 });
    }
    const agg = productMap.get(row.product);
    agg.sales += lineTotal;
    agg.quantity += row.quantity;
  }

  const dailySales = Array.from(dailyMap.entries())
    .map(([date, sales]) => ({ date, sales: normalizeNumber(sales) }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const categorySales = Array.from(categoryMap.entries())
    .map(([category, sales]) => ({ category, sales: normalizeNumber(sales) }))
    .sort((a, b) => b.sales - a.sales || a.category.localeCompare(b.category));

  const topProducts = Array.from(productMap.entries())
    .map(([product, agg]) => ({
      product,
      sales: normalizeNumber(agg.sales),
      quantity: agg.quantity,
    }))
    .sort((a, b) => b.sales - a.sales || a.product.localeCompare(b.product))
    .slice(0, 3);

  return {
    daily_sales: dailySales,
    category_sales: categorySales,
    top_products: topProducts,
  };
}

/**
 * 2配列をインデックス単位で比較し、差分だけ返す。
 *
 * @param {Array<object>} jsonItems - JSON集計配列。
 * @param {Array<object>} parquetItems - Parquet集計配列。
 * @returns {Array<{index: number, json_value: object | null, parquet_value: object | null}>} 差分配列。
 */
function compareItems(jsonItems, parquetItems) {
  const differences = [];
  const maxLen = Math.max(jsonItems.length, parquetItems.length);
  for (let i = 0; i < maxLen; i += 1) {
    const left = i < jsonItems.length ? jsonItems[i] : null;
    const right = i < parquetItems.length ? parquetItems[i] : null;
    if (JSON.stringify(left) !== JSON.stringify(right)) {
      differences.push({
        index: i,
        json_value: left,
        parquet_value: right,
      });
    }
  }
  return differences;
}

/**
 * 比較結果JSONを組み立てる。
 *
 * @param {Array<{date: string, product: string, category: string, quantity: number, price: number}>} jsonRows - JSON明細。
 * @param {Array<{date: string, product: string, category: string, quantity: number, price: number}>} parquetRows - Parquet明細。
 * @returns {{summary: {json_record_count: number, parquet_record_count: number, is_equivalent: boolean}, json_aggregations: object, parquet_aggregations: object, differences: object}} 結果JSON。
 */
function buildResult(jsonRows, parquetRows) {
  const jsonAggregations = aggregateSales(jsonRows);
  const parquetAggregations = aggregateSales(parquetRows);

  const differences = {
    daily_sales: compareItems(jsonAggregations.daily_sales, parquetAggregations.daily_sales),
    category_sales: compareItems(
      jsonAggregations.category_sales,
      parquetAggregations.category_sales
    ),
    top_products: compareItems(jsonAggregations.top_products, parquetAggregations.top_products),
  };

  const isEquivalent =
    differences.daily_sales.length === 0 &&
    differences.category_sales.length === 0 &&
    differences.top_products.length === 0;

  return {
    summary: {
      // JSON入力の件数。
      json_record_count: jsonRows.length,
      // Parquet入力の件数。
      parquet_record_count: parquetRows.length,
      // 集計差分が全軸で0件か。
      is_equivalent: isEquivalent,
    },
    json_aggregations: jsonAggregations,
    parquet_aggregations: parquetAggregations,
    differences,
  };
}

/**
 * CLIエントリーポイント。
 *
 * @returns {Promise<void>}
 */
async function main() {
  const { jsonInput, parquetInput, debug } = parseArgs(process.argv);

  const jsonRows = readJsonSales(jsonInput);
  const parquetRows = await readParquetSales(parquetInput);

  if (debug) {
    process.stderr.write(`[debug] jsonRows=${JSON.stringify(jsonRows, null, 2)}\n`);
    process.stderr.write(`[debug] parquetRows=${JSON.stringify(parquetRows, null, 2)}\n`);
  }

  const result = buildResult(jsonRows, parquetRows);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  const message =
    error && error.stack ? error.stack : error && error.message ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
