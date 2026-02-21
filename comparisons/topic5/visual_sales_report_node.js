const fs = require("fs");
const path = require("path");

// このファイルは「お題5: 可視化によるレポート化」の Node.js 実装。
// Node.js ではサーバーサイド単体だと可視化ライブラリを追加することが多いため、
// ここでは学習用にSVGを手組みして、Python(matplotlib)との記述差を比較する。

/**
 * CLI引数を解析する。
 *
 * @param {string[]} argv - プロセス引数。
 * @returns {{ input: string, outputDir: string }} 実行オプション。
 */
function parseArgs(argv) {
  let input = "";
  let outputDir = "";
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--input") {
      input = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--output-dir") {
      outputDir = argv[i + 1] || "";
      i += 1;
    }
  }
  if (!input) {
    throw new Error("必須引数がありません: --input <csv_path>");
  }
  if (!outputDir) {
    throw new Error("必須引数がありません: --output-dir <dir_path>");
  }
  return { input, outputDir };
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
 * CSVを読み込み、売上明細配列へ変換する。
 *
 * @param {string} inputPath - CSVパス。
 * @returns {Array<{date: string, product: string, category: string, quantity: number, price: number, line_total: number}>} 明細配列。
 */
function readSalesCsv(inputPath) {
  const raw = fs.readFileSync(inputPath, "utf8").trim();
  const lines = raw.split(/\r?\n/);
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const [date, product, category, quantityRaw, priceRaw] = lines[i].split(",");
    const quantity = Number(quantityRaw);
    const price = Number(priceRaw);
    rows.push({
      date,
      product,
      category,
      quantity,
      price,
      line_total: quantity * price,
    });
  }
  return rows;
}

/**
 * 単純な縦棒グラフSVGを作成する。
 *
 * @param {string} title - グラフタイトル。
 * @param {Array<{label: string, value: number}>} points - 描画データ。
 * @returns {string} SVG文字列。
 */
function buildBarSvg(title, points) {
  const width = 720;
  const height = 420;
  const margin = { top: 50, right: 30, bottom: 70, left: 70 };
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;
  const maxValue = Math.max(...points.map((p) => p.value), 1);
  const barW = plotW / points.length;

  let bars = "";
  let labels = "";
  for (let i = 0; i < points.length; i += 1) {
    const point = points[i];
    const h = (point.value / maxValue) * plotH;
    const x = margin.left + i * barW + 8;
    const y = margin.top + (plotH - h);
    bars += `<rect x="${x}" y="${y}" width="${Math.max(barW - 16, 8)}" height="${h}" fill="#3f7cac" />`;
    labels += `<text x="${margin.left + i * barW + barW / 2}" y="${height - 25}" text-anchor="middle" font-size="11">${point.label}</text>`;
  }

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#ffffff"/>
  <text x="${width / 2}" y="28" text-anchor="middle" font-size="18">${title}</text>
  <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotH}" stroke="#444"/>
  <line x1="${margin.left}" y1="${margin.top + plotH}" x2="${margin.left + plotW}" y2="${margin.top + plotH}" stroke="#444"/>
  ${bars}
  ${labels}
</svg>
`;
}

/**
 * 折れ線グラフSVGを作成する。
 *
 * @param {string} title - グラフタイトル。
 * @param {Array<{label: string, value: number}>} points - 描画データ。
 * @returns {string} SVG文字列。
 */
function buildLineSvg(title, points) {
  const width = 720;
  const height = 420;
  const margin = { top: 50, right: 30, bottom: 70, left: 70 };
  const plotW = width - margin.left - margin.right;
  const plotH = height - margin.top - margin.bottom;
  const maxValue = Math.max(...points.map((p) => p.value), 1);
  const stepX = points.length > 1 ? plotW / (points.length - 1) : plotW;

  const polyline = points
    .map((point, i) => {
      const x = margin.left + i * stepX;
      const y = margin.top + plotH - (point.value / maxValue) * plotH;
      return `${x},${y}`;
    })
    .join(" ");

  const labels = points
    .map((point, i) => {
      const x = margin.left + i * stepX;
      return `<text x="${x}" y="${height - 25}" text-anchor="middle" font-size="11">${point.label}</text>`;
    })
    .join("");

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
  <rect x="0" y="0" width="${width}" height="${height}" fill="#ffffff"/>
  <text x="${width / 2}" y="28" text-anchor="middle" font-size="18">${title}</text>
  <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotH}" stroke="#444"/>
  <line x1="${margin.left}" y1="${margin.top + plotH}" x2="${margin.left + plotW}" y2="${margin.top + plotH}" stroke="#444"/>
  <polyline fill="none" stroke="#2a9d8f" stroke-width="3" points="${polyline}"/>
  ${labels}
</svg>
`;
}

/**
 * 意思決定向けの短い示唆を作成する。
 *
 * @param {{best_sales_day: string, best_sales_amount: number, total_orders: number, total_revenue: number, average_order_value: number}} summary - サマリ。
 * @param {Array<{category: string, sales: number}>} categorySales - カテゴリ集計。
 * @param {Array<{product: string, sales: number, quantity: number}>} topProducts - 商品集計。
 * @returns {string[]} 示唆配列。
 */
function buildInsights(summary, categorySales, topProducts) {
  return [
    `売上最大日は ${summary.best_sales_day} で、日次売上は ${summary.best_sales_amount} です。`,
    `カテゴリ別では ${categorySales[0].category} が最大で、売上は ${categorySales[0].sales} です。`,
    `商品別では ${topProducts[0].product} が最大で、売上は ${topProducts[0].sales} です。`,
  ];
}

/**
 * Markdownレポートを出力する。
 *
 * @param {string} reportPath - レポートパス。
 * @param {object} summary - サマリ。
 * @param {string[]} insights - 示唆。
 * @param {{daily_sales_chart: string, category_sales_chart: string, top_products_chart: string, decision_report_markdown: string}} artifacts - 生成物。
 * @returns {void}
 */
function writeMarkdownReport(reportPath, summary, insights, artifacts) {
  const lines = [
    "# お題5 可視化レポート",
    "",
    "## サマリ",
    `- 総注文件数: ${summary.total_orders}`,
    `- 全売上: ${summary.total_revenue}`,
    `- 注文平均売上: ${summary.average_order_value}`,
    `- 売上最大日: ${summary.best_sales_day} (${summary.best_sales_amount})`,
    "",
    "## 意思決定メモ",
    ...insights.map((item) => `- ${item}`),
    "",
    "## 生成グラフ",
    `- 日次売上: \`${artifacts.daily_sales_chart}\``,
    `- カテゴリ売上: \`${artifacts.category_sales_chart}\``,
    `- 上位商品: \`${artifacts.top_products_chart}\``,
    "",
  ];
  fs.writeFileSync(reportPath, lines.join("\n"), "utf8");
}

/**
 * 可視化・集計・レポートを実行して結果JSONを返す。
 *
 * @param {string} inputPath - 入力CSV。
 * @param {string} outputDir - 出力ディレクトリ。
 * @returns {{
 * summary: {total_orders: number, total_revenue: number, average_order_value: number, best_sales_day: string, best_sales_amount: number},
 * daily_sales: Array<{date: string, sales: number}>,
 * category_sales: Array<{category: string, sales: number}>,
 * top_products: Array<{product: string, sales: number, quantity: number}>,
 * insights: string[],
 * artifacts: {daily_sales_chart: string, category_sales_chart: string, top_products_chart: string, decision_report_markdown: string}
 * }} 結果JSON。
 */
function buildVisualSalesReport(inputPath, outputDir) {
  const rows = readSalesCsv(inputPath);
  fs.mkdirSync(outputDir, { recursive: true });

  const dailyMap = new Map();
  const categoryMap = new Map();
  const productMap = new Map();
  for (const row of rows) {
    dailyMap.set(row.date, (dailyMap.get(row.date) || 0) + row.line_total);
    categoryMap.set(row.category, (categoryMap.get(row.category) || 0) + row.line_total);
    if (!productMap.has(row.product)) {
      productMap.set(row.product, { sales: 0, quantity: 0 });
    }
    const agg = productMap.get(row.product);
    agg.sales += row.line_total;
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

  let best = dailySales[0];
  for (const item of dailySales) {
    if (item.sales > best.sales) {
      best = item;
    }
  }
  const totalRevenue = normalizeNumber(dailySales.reduce((acc, item) => acc + item.sales, 0));
  const summary = {
    // 入力CSVの注文明細数。
    total_orders: rows.length,
    // 全売上合計。
    total_revenue: totalRevenue,
    // 注文1件あたり平均売上。
    average_order_value: normalizeNumber(totalRevenue / rows.length),
    // 売上最大日。
    best_sales_day: best.date,
    // 売上最大日の売上。
    best_sales_amount: best.sales,
  };

  const artifacts = {
    // 日次売上グラフファイル名。
    daily_sales_chart: "daily_sales.svg",
    // カテゴリ売上グラフファイル名。
    category_sales_chart: "category_sales.svg",
    // 上位商品グラフファイル名。
    top_products_chart: "top_products.svg",
    // 意思決定レポートファイル名。
    decision_report_markdown: "decision_report.md",
  };

  const insights = buildInsights(summary, categorySales, topProducts);

  const dailySvg = buildLineSvg(
    "Daily Sales Trend",
    dailySales.map((item) => ({ label: item.date, value: Number(item.sales) }))
  );
  const categorySvg = buildBarSvg(
    "Category Sales",
    categorySales.map((item) => ({ label: item.category, value: Number(item.sales) }))
  );
  const productSvg = buildBarSvg(
    "Top Products by Sales",
    topProducts.map((item) => ({ label: item.product, value: Number(item.sales) }))
  );

  fs.writeFileSync(path.join(outputDir, artifacts.daily_sales_chart), dailySvg, "utf8");
  fs.writeFileSync(path.join(outputDir, artifacts.category_sales_chart), categorySvg, "utf8");
  fs.writeFileSync(path.join(outputDir, artifacts.top_products_chart), productSvg, "utf8");
  writeMarkdownReport(path.join(outputDir, artifacts.decision_report_markdown), summary, insights, artifacts);

  return {
    // 売上全体の主要指標。
    summary,
    // 日単位の売上推移。
    daily_sales: dailySales,
    // カテゴリ単位の売上。
    category_sales: categorySales,
    // 売上上位商品の明細。
    top_products: topProducts,
    // 意思決定のための短い示唆。
    insights,
    // 生成したグラフ/レポートのファイル名。
    artifacts,
  };
}

function main() {
  const { input, outputDir } = parseArgs(process.argv);
  const result = buildVisualSalesReport(input, outputDir);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main();
