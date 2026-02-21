const fs = require("fs");

// このファイルは「お題3: 複数データの結合（JOIN相当）」の Node.js 実装。
// Node.js では、顧客マスタを Map に載せて注文を突合することで JOIN を表現する。
// Python の pandas.merge、PHP の連想配列突合と比較するため、処理を明示的に書く。

/**
 * CLI引数を解析する。
 *
 * @param {string[]} argv - プロセス引数。
 * @returns {{ customers: string, orders: string }} 入力パス。
 */
function parseArgs(argv) {
  let customers = "";
  let orders = "";
  let debug = false;
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i] === "--customers") {
      customers = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--orders") {
      orders = argv[i + 1] || "";
      i += 1;
    } else if (argv[i] === "--debug") {
      debug = true;
    }
  }
  if (!customers || !orders) {
    throw new Error("必須引数がありません: --customers <csv_path> --orders <csv_path>");
  }
  return { customers, orders, debug };
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
 * ヘッダー付きCSVを簡易パースする（学習用の最小実装）。
 *
 * @param {string} path - CSVパス。
 * @returns {Array<Record<string, string>>} 行配列。
 */
function readCsv(path) {
  // 学習用に簡易CSVパーサーを使う。
  // 実務では quoted field などを考慮して csv-parse 等を使うのが一般的。
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
 * セグメント売上サマリを作成する。
 *
 * @param {Array<{segment: string, line_total: number, customer_id: string}>} rows - JOIN済み行。
 * @returns {Array<{segment: string, total_sales: number, order_count: number, avg_order_amount: number, unique_customers: number}>} 集計結果。
 */
function buildSegmentSales(rows) {
  // Node.js では Map を集計バッファとして使い、1パスで更新する。
  // Python だと groupby/agg、PHP だと連想配列の加算で同じ目的を達成する。
  const segmentMap = new Map();
  for (const row of rows) {
    if (!segmentMap.has(row.segment)) {
      segmentMap.set(row.segment, {
        total_sales: 0,
        order_count: 0,
        customerIds: new Set(),
      });
    }
    const agg = segmentMap.get(row.segment);
    agg.total_sales += row.line_total;
    agg.order_count += 1;
    agg.customerIds.add(row.customer_id);
  }

  return Array.from(segmentMap.entries())
    .map(([segment, agg]) => ({
      segment,
      total_sales: normalizeNumber(agg.total_sales),
      order_count: agg.order_count,
      avg_order_amount: normalizeNumber(agg.total_sales / agg.order_count),
      unique_customers: agg.customerIds.size,
    }))
    .sort((a, b) => b.total_sales - a.total_sales || a.segment.localeCompare(b.segment));
}

/**
 * 顧客CSVと注文CSVを突合し、INNER/LEFT JOIN相当の分析結果を返す。
 *
 * @param {string} customersPath - 顧客CSVパス。
 * @param {string} ordersPath - 注文CSVパス。
 * @returns {{
 * summary: {customers_count: number, orders_count: number, inner_join_rows: number, left_join_rows: number, orphan_order_count: number},
 * segment_sales_inner: Array<{segment: string, total_sales: number, order_count: number, avg_order_amount: number, unique_customers: number}>,
 * segment_sales_left: Array<{segment: string, total_sales: number, order_count: number, avg_order_amount: number, unique_customers: number}>,
 * top_products_by_segment_inner: Array<{segment: string, product: string, total_sales: number, total_quantity: number}>,
 * orphan_orders: Array<{order_id: string, order_date: string, customer_id: string, product: string, line_total: number}>
 * }} 結果JSON。
 */
function joinAndAnalyze(customersPath, ordersPath, debug = false) {
  // 行程1: 読み込み後すぐに型を確定する。
  // quantity/unit_price を数値化し、line_total を共通列として先に作成する。
  const customers = readCsv(customersPath);
  const orders = readCsv(ordersPath).map((row) => ({
    ...row,
    quantity: Number(row.quantity),
    unit_price: Number(row.unit_price),
    line_total: Number(row.quantity) * Number(row.unit_price),
  }));
  debugLog(debug, "customers 読み込み直後", customers);
  debugLog(debug, "orders 読み込み直後(line_total込み)", orders);

  // Node.js での JOIN 表現:
  // 1) 顧客マスタを Map 化
  // 2) 注文を1件ずつ突合
  // Python だと pandas.merge(on="customer_id", how="inner/left") を使える。
  const customerMap = new Map();
  for (const customer of customers) {
    customerMap.set(customer.customer_id, customer);
  }

  const innerJoined = [];
  const leftJoined = [];
  const orphanOrders = [];

  // 行程2: INNER JOIN / LEFT JOIN 相当の突合。
  // INNER: 顧客マスタに存在する注文のみ採用。
  // LEFT: 注文全件を採用し、未突合は segment='Unknown' に寄せる。
  for (const order of orders) {
    const customer = customerMap.get(order.customer_id);
    if (customer) {
      const joined = {
        ...order,
        customer_name: customer.customer_name,
        segment: customer.segment,
      };
      innerJoined.push(joined);
      leftJoined.push(joined);
    } else {
      const unknownJoined = {
        ...order,
        customer_name: null,
        segment: "Unknown",
      };
      leftJoined.push(unknownJoined);
      orphanOrders.push({
        order_id: order.order_id,
        order_date: order.order_date,
        customer_id: order.customer_id,
        product: order.product,
        line_total: normalizeNumber(order.line_total),
      });
    }
  }
  debugLog(debug, "innerJoined", innerJoined);
  debugLog(debug, "leftJoined", leftJoined);
  debugLog(debug, "orphanOrders", orphanOrders);

  const segmentSalesInner = buildSegmentSales(innerJoined);
  const segmentSalesLeft = buildSegmentSales(leftJoined);
  debugLog(debug, "segmentSalesInner", segmentSalesInner);
  debugLog(debug, "segmentSalesLeft", segmentSalesLeft);

  // セグメント内トップ商品:
  // まず segment x product 集計を作り、各セグメントで売上最大の1件を採用する。
  const segmentProductMap = new Map();
  for (const row of innerJoined) {
    const key = `${row.segment}::${row.product}`;
    if (!segmentProductMap.has(key)) {
      segmentProductMap.set(key, {
        segment: row.segment,
        product: row.product,
        total_sales: 0,
        total_quantity: 0,
      });
    }
    const agg = segmentProductMap.get(key);
    agg.total_sales += row.line_total;
    agg.total_quantity += row.quantity;
  }

  const groupedBySegment = new Map();
  for (const item of segmentProductMap.values()) {
    if (!groupedBySegment.has(item.segment)) {
      groupedBySegment.set(item.segment, []);
    }
    groupedBySegment.get(item.segment).push(item);
  }

  const topProductsBySegmentInner = Array.from(groupedBySegment.entries())
    .map(([segment, items]) => {
      // 各セグメント内で「売上降順、同率なら商品名昇順」に固定する。
      // 並び順を固定すると、3言語間diffが安定する。
      items.sort((a, b) => b.total_sales - a.total_sales || a.product.localeCompare(b.product));
      const top = items[0];
      return {
        segment,
        product: top.product,
        total_sales: normalizeNumber(top.total_sales),
        total_quantity: top.total_quantity,
      };
    })
    .sort((a, b) => b.total_sales - a.total_sales || a.segment.localeCompare(b.segment));
  debugLog(debug, "topProductsBySegmentInner", topProductsBySegmentInner);

  orphanOrders.sort(
    (a, b) => a.order_date.localeCompare(b.order_date) || a.order_id.localeCompare(b.order_id)
  );

  // 行程3: JSONスキーマを固定して返す。
  // 返却キーを固定することで、実装差ではなく言語差の比較に集中できる。
  return {
    // 集計処理全体の件数サマリ。
    summary: {
      // 顧客マスタCSVの総件数。
      customers_count: customers.length,
      // 注文CSVの総件数。
      orders_count: orders.length,
      // INNER JOINで突合できた注文件数。
      inner_join_rows: innerJoined.length,
      // LEFT JOINで保持した注文件数（orders総件数と同値）。
      left_join_rows: leftJoined.length,
      // 顧客マスタ未登録の注文件数。
      orphan_order_count: orphanOrders.length,
    },
    // INNER JOINベースのセグメント売上集計。
    segment_sales_inner: segmentSalesInner,
    // LEFT JOINベースのセグメント売上集計（Unknown含む）。
    segment_sales_left: segmentSalesLeft,
    // INNER JOINデータから算出したセグメント別トップ商品。
    top_products_by_segment_inner: topProductsBySegmentInner,
    // 顧客マスタ未登録注文の明細一覧。
    orphan_orders: orphanOrders,
  };
}

function main() {
  const { customers, orders, debug } = parseArgs(process.argv);
  const result = joinAndAnalyze(customers, orders, debug);
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main();
