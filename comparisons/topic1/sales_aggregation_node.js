const fs = require("fs");

// このファイルは「お題1: CSV売上集計」の Node.js 実装。
// 目的は Python/PHP と同一入力・同一出力スキーマで比較できること。
// あえて外部CSVライブラリを使わず、配列とMapで集計を手実装している。

// 3言語共通I/F: --input <csv_path> を受け取る。
/**
 * CLI引数を解析して入力CSVパスを取り出す。
 *
 * @param {string[]} argv - プロセスの引数配列。
 * @returns {{ input: string }} 解析済み引数。
 * @throws {Error} --input が未指定の場合。
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
    throw new Error("必須引数がありません: --input <csv_path>");
  }
  return { input };
}

/**
 * 金額を小数第2位へ丸めて正規化する。
 *
 * @param {number} value - 正規化前の金額。
 * @returns {number} 正規化後の金額。
 */
function money(value) {
  // 金額は小数2桁に丸め、整数値は整数表現に寄せる。
  // 例: 25 -> 25, 22.5 -> 22.5
  return Number(value.toFixed(2));
}

/**
 * CSV読み込みから集計・並び替え・JSON出力までを実行する。
 *
 * @returns {void}
 */
function main() {
  const { input } = parseArgs(process.argv);

  // 依存ライブラリなしで比較するため、CSVを最小実装でパースする。
  // 1行目はヘッダーなので、lines.slice(1) でデータ行のみ扱う。
  const raw = fs.readFileSync(input, "utf8").trim();
  const lines = raw.split(/\r?\n/);
  const rows = lines.slice(1).map((line) => {
    // CSV列順は以下固定:
    // date, product, category, quantity, price
    const [date, product, category, quantityRaw, priceRaw] = line.split(",");
    const quantity = Number(quantityRaw);
    const price = Number(priceRaw);
    // lineTotal（明細売上）を先に作っておくと、以降の集計が単純化できる。
    return { date, product, category, quantity, price, lineTotal: quantity * price };
  });

  // 3種類の集計バッファ:
  // - dailyMap: 日付 -> 売上合計
  // - categoryMap: カテゴリ -> 売上合計
  // - productMap: 商品 -> { sales, quantity }
  const dailyMap = new Map();
  const categoryMap = new Map();
  const productMap = new Map();

  // 1パスで日次・カテゴリ・商品別の3集計を同時に作る。
  for (const row of rows) {
    dailyMap.set(row.date, (dailyMap.get(row.date) || 0) + row.lineTotal);
    categoryMap.set(
      row.category,
      (categoryMap.get(row.category) || 0) + row.lineTotal
    );

    if (!productMap.has(row.product)) {
      // 初登場商品は初期値を作る。
      productMap.set(row.product, { sales: 0, quantity: 0 });
    }
    const productAgg = productMap.get(row.product);
    // 商品ごとの売上合計・数量合計を更新。
    productAgg.sales += row.lineTotal;
    productAgg.quantity += row.quantity;
  }

  // 日次売上を配列へ変換し、日付昇順に固定。
  const dailySales = Array.from(dailyMap.entries())
    .map(([date, sales]) => ({ date, sales: money(sales) }))
    // 出力順の揺れを防ぐため、日付昇順で固定する。
    .sort((a, b) => a.date.localeCompare(b.date));

  // カテゴリ売上を配列へ変換し、売上降順 + カテゴリ名昇順で固定。
  const categorySales = Array.from(categoryMap.entries())
    .map(([category, sales]) => ({ category, sales: money(sales) }))
    // 売上降順、同額時はカテゴリ名昇順。
    .sort((a, b) => b.sales - a.sales || a.category.localeCompare(b.category));

  // 商品別集計を配列へ変換し、売上上位3件に絞る。
  const topProducts = Array.from(productMap.entries())
    .map(([product, agg]) => ({
      product,
      sales: money(agg.sales),
      quantity: agg.quantity,
    }))
    .sort((a, b) => b.sales - a.sales || a.product.localeCompare(b.product))
    // 売上Top3に絞ってPython/PHP実装と同じ仕様に合わせる。
    .slice(0, 3);

  // 3言語で同一のJSONキー構造に統一する。
  // 比較検証時に「処理差」ではなく「実装差」だけを見られるようにするため。
  const result = {
    daily_sales: dailySales,
    category_sales: categorySales,
    top_products: topProducts,
  };

  // 人間が比較しやすいよう pretty print で出力。
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

// Node.js 実行エントリーポイント。
main();
