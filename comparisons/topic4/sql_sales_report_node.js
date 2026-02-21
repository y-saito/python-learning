const { Client } = require("pg");

// このファイルは「お題4: SQL連携と分析クエリ」の Node.js 実装。
// Node.js では pg で rows を取得し、Map や配列操作で再集計する。
// Python の pandas 集計、PHP の PDO+foreach 集計と比較するために処理を明示する。

/**
 * CLI引数を解析する。
 *
 * @param {string[]} argv - プロセス引数。
 * @returns {{
 * host: string,
 * port: number,
 * database: string,
 * user: string,
 * password: string,
 * highValueThreshold: number,
 * debug: boolean
 * }} 接続設定と実行オプション。
 */
function parseArgs(argv) {
  const options = {
    host: "host.docker.internal",
    port: 5432,
    database: "app",
    user: "app",
    password: "app",
    highValueThreshold: 500,
    debug: false,
  };

  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--host") {
      options.host = argv[i + 1] || options.host;
      i += 1;
    } else if (argv[i] === "--port") {
      options.port = Number(argv[i + 1] || options.port);
      i += 1;
    } else if (argv[i] === "--database") {
      options.database = argv[i + 1] || options.database;
      i += 1;
    } else if (argv[i] === "--user") {
      options.user = argv[i + 1] || options.user;
      i += 1;
    } else if (argv[i] === "--password") {
      options.password = argv[i + 1] || options.password;
      i += 1;
    } else if (argv[i] === "--high-value-threshold") {
      options.highValueThreshold = Number(argv[i + 1] || options.highValueThreshold);
      i += 1;
    } else if (argv[i] === "--debug") {
      options.debug = true;
    }
  }

  return options;
}

/**
 * 数値を比較しやすいJSON表現へ正規化する。
 *
 * @param {number} value - 正規化前の数値。
 * @returns {number} 正規化後の数値。
 */
function normalizeNumber(value) {
  const rounded = Number(value.toFixed(2));
  if (Number.isInteger(rounded)) {
    return rounded;
  }
  return rounded;
}

/**
 * デバッグログを標準エラーへ出力する。
 *
 * @param {boolean} debug - デバッグ有無。
 * @param {string} title - ログタイトル。
 * @param {unknown} value - 表示対象。
 * @returns {void}
 */
function debugLog(debug, title, value) {
  if (!debug) {
    return;
  }
  process.stderr.write(`DEBUG: ${title}\n`);
  process.stderr.write(`${JSON.stringify(value, null, 2)}\n`);
}

/**
 * 売上データを集計してお題4の固定JSONスキーマを返す。
 *
 * @param {Array<{order_id: string, order_date: string, customer_segment: string, payment_method: string, order_amount: string | number}>} rows - SQL抽出結果。
 * @param {number} highValueThreshold - 高額注文しきい値。
 * @param {boolean} debug - デバッグ有無。
 * @returns {{
 * summary: {total_rows: number, date_range_start: string, date_range_end: string, total_revenue: number, high_value_order_count: number},
 * daily_sales: Array<{date: string, sales: number}>,
 * segment_sales: Array<{segment: string, total_sales: number, order_count: number, avg_order_amount: number}>,
 * payment_method_sales: Array<{payment_method: string, total_sales: number, order_count: number, avg_order_amount: number}>,
 * high_value_orders: Array<{order_id: string, order_date: string, segment: string, payment_method: string, order_amount: number}>
 * }} 集計結果。
 */
function aggregateRows(rows, highValueThreshold, debug) {
  if (rows.length === 0) {
    throw new Error("sales_orders にデータがありません。先にシードSQLを投入してください。");
  }

  const normalizedRows = rows.map((row) => ({
    order_id: row.order_id,
    order_date: row.order_date,
    customer_segment: row.customer_segment,
    payment_method: row.payment_method,
    order_amount: Number(row.order_amount),
  }));
  debugLog(debug, "SQL抽出直後のrows", normalizedRows);

  const dailyMap = new Map();
  const segmentMap = new Map();
  const paymentMap = new Map();
  let totalRevenue = 0;

  for (const row of normalizedRows) {
    totalRevenue += row.order_amount;

    if (!dailyMap.has(row.order_date)) {
      dailyMap.set(row.order_date, 0);
    }
    dailyMap.set(row.order_date, dailyMap.get(row.order_date) + row.order_amount);

    if (!segmentMap.has(row.customer_segment)) {
      segmentMap.set(row.customer_segment, { total_sales: 0, order_count: 0 });
    }
    const segmentAgg = segmentMap.get(row.customer_segment);
    segmentAgg.total_sales += row.order_amount;
    segmentAgg.order_count += 1;

    if (!paymentMap.has(row.payment_method)) {
      paymentMap.set(row.payment_method, { total_sales: 0, order_count: 0 });
    }
    const paymentAgg = paymentMap.get(row.payment_method);
    paymentAgg.total_sales += row.order_amount;
    paymentAgg.order_count += 1;
  }

  const dailySales = Array.from(dailyMap.entries())
    .map(([date, sales]) => ({
      // 売上日。
      date,
      // その日の売上合計。
      sales: normalizeNumber(sales),
    }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const segmentSales = Array.from(segmentMap.entries())
    .map(([segment, agg]) => ({
      // 顧客セグメント名。
      segment,
      // セグメント売上合計。
      total_sales: normalizeNumber(agg.total_sales),
      // セグメント内の注文件数。
      order_count: agg.order_count,
      // セグメント内の平均注文金額。
      avg_order_amount: normalizeNumber(agg.total_sales / agg.order_count),
    }))
    .sort((a, b) => b.total_sales - a.total_sales || a.segment.localeCompare(b.segment));

  const paymentMethodSales = Array.from(paymentMap.entries())
    .map(([paymentMethod, agg]) => ({
      // 決済手段名。
      payment_method: paymentMethod,
      // 決済手段ごとの売上合計。
      total_sales: normalizeNumber(agg.total_sales),
      // 決済手段ごとの注文件数。
      order_count: agg.order_count,
      // 決済手段ごとの平均注文金額。
      avg_order_amount: normalizeNumber(agg.total_sales / agg.order_count),
    }))
    .sort((a, b) => b.total_sales - a.total_sales || a.payment_method.localeCompare(b.payment_method));

  const highValueOrders = normalizedRows
    .filter((row) => row.order_amount >= highValueThreshold)
    .map((row) => ({
      // 注文ID。
      order_id: row.order_id,
      // 注文日。
      order_date: row.order_date,
      // 注文顧客のセグメント。
      segment: row.customer_segment,
      // 注文時の決済手段。
      payment_method: row.payment_method,
      // 注文金額（しきい値以上）。
      order_amount: normalizeNumber(row.order_amount),
    }))
    .sort(
      (a, b) =>
        b.order_amount - a.order_amount ||
        a.order_id.localeCompare(b.order_id) ||
        a.order_date.localeCompare(b.order_date)
    );

  debugLog(debug, "dailySales", dailySales);
  debugLog(debug, "segmentSales", segmentSales);
  debugLog(debug, "paymentMethodSales", paymentMethodSales);
  debugLog(debug, "highValueOrders", highValueOrders);

  const sortedDates = dailySales.map((item) => item.date);
  return {
    // レコード件数・期間・総売上の要約。
    summary: {
      // SQL抽出後の総件数。
      total_rows: normalizedRows.length,
      // 集計期間の開始日。
      date_range_start: sortedDates[0],
      // 集計期間の終了日。
      date_range_end: sortedDates[sortedDates.length - 1],
      // 全注文の売上合計。
      total_revenue: normalizeNumber(totalRevenue),
      // 高額注文（しきい値以上）の件数。
      high_value_order_count: highValueOrders.length,
    },
    // 日次売上配列。
    daily_sales: dailySales,
    // セグメント別売上配列。
    segment_sales: segmentSales,
    // 決済手段別売上配列。
    payment_method_sales: paymentMethodSales,
    // 高額注文の明細配列。
    high_value_orders: highValueOrders,
  };
}

async function main() {
  const options = parseArgs(process.argv);
  const client = new Client({
    host: options.host,
    port: options.port,
    database: options.database,
    user: options.user,
    password: options.password,
  });

  await client.connect();
  try {
    // Node.js:
    //   SQLはDBで実行し、分析寄りの再集計をアプリ側で行う流れを体験する。
    // Python は read_sql_query 後に DataFrame で、PHP は PDO fetchAll 後に配列で同等処理を行う。
    const query = `
      SELECT
        order_id,
        order_date::text AS order_date,
        customer_segment,
        payment_method,
        order_amount
      FROM sales_orders
      ORDER BY order_date, order_id
    `;
    const result = await client.query(query);
    const output = aggregateRows(result.rows, options.highValueThreshold, options.debug);
    process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exit(1);
});
