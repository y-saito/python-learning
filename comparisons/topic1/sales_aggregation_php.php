<?php

// このファイルは「お題1: CSV売上集計」の PHP 実装。
// 目的は Python/Node.js と同じ入力CSVを処理し、同一JSONスキーマを返すこと。
// 実装は fgetcsv + 連想配列の手続き処理で、比較学習しやすくしている。

// 3言語共通I/F: --input <csv_path> を受け取る。
/**
 * CLI引数を解析して入力CSVパスを返す。
 *
 * @param array<int, string> $argv CLI引数配列。
 *
 * @return string 入力CSVのパス。
 *
 * @throws InvalidArgumentException --input が未指定の場合。
 */
function parse_input_path(array $argv): string
{
    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--input' && isset($argv[$i + 1])) {
            return $argv[$i + 1];
        }
    }

    throw new InvalidArgumentException('必須引数がありません: --input <csv_path>');
}

/**
 * 金額を小数第2位へ丸めて正規化する。
 *
 * @param float $value 正規化前の金額。
 *
 * @return float 正規化後の金額。
 */
function money(float $value): float
{
    // 金額表現は小数2桁に統一する。
    // 例: 25.0 は json_encode 時に 25 として出力される。
    return round($value, 2);
}

/**
 * 売上CSVを日次・カテゴリ別・商品上位で集計する。
 *
 * @param string $inputPath 入力CSVのパス。
 *
 * @return array<string, array<int, array<string, int|float|string>>>
 *
 * @throws RuntimeException ファイルを開けない、またはヘッダーがない場合。
 */
function aggregate_sales(string $inputPath): array
{
    // CSVファイルを開く。開けない場合は即例外で停止。
    $handle = fopen($inputPath, 'r');
    if ($handle === false) {
        throw new RuntimeException("入力ファイルを開けません: {$inputPath}");
    }

    // 1行目はヘッダー。空ファイル対策として存在を確認する。
    $header = fgetcsv($handle);
    if ($header === false) {
        fclose($handle);
        throw new RuntimeException("CSVヘッダーが見つかりません: {$inputPath}");
    }

    // 3種類の集計バッファ:
    // - $daily: 日付 => 売上合計
    // - $category: カテゴリ => 売上合計
    // - $product: 商品 => ['sales' => 売上合計, 'quantity' => 数量合計]
    $daily = [];
    $category = [];
    $product = [];

    // 1回の走査で日次・カテゴリ・商品別の3集計を同時に構築する。
    while (($row = fgetcsv($handle)) !== false) {
        // CSV列順は以下固定:
        // date, product, category, quantity, price
        [$date, $productName, $categoryName, $quantityRaw, $priceRaw] = $row;
        $quantity = (int)$quantityRaw;
        $price = (float)$priceRaw;
        // 明細売上 = 数量 * 単価
        $lineTotal = $quantity * $price;

        // 日次売上を更新。
        if (!isset($daily[$date])) {
            $daily[$date] = 0.0;
        }
        $daily[$date] += $lineTotal;

        // カテゴリ売上を更新。
        if (!isset($category[$categoryName])) {
            $category[$categoryName] = 0.0;
        }
        $category[$categoryName] += $lineTotal;

        // 商品別売上・数量を更新。
        if (!isset($product[$productName])) {
            $product[$productName] = ['sales' => 0.0, 'quantity' => 0];
        }
        $product[$productName]['sales'] += $lineTotal;
        $product[$productName]['quantity'] += $quantity;
    }

    fclose($handle);

    // 日次売上は日付昇順で安定化。
    ksort($daily);
    $dailySales = [];
    foreach ($daily as $date => $sales) {
        // JSON出力しやすい配列形式へ変換。
        $dailySales[] = ['date' => $date, 'sales' => money($sales)];
    }

    $categorySales = [];
    foreach ($category as $categoryName => $sales) {
        // JSON出力しやすい配列形式へ変換。
        $categorySales[] = ['category' => $categoryName, 'sales' => money($sales)];
    }
    // カテゴリ売上は売上降順、同額時はカテゴリ名昇順。
    usort($categorySales, function (array $a, array $b): int {
        if ($a['sales'] === $b['sales']) {
            return strcmp($a['category'], $b['category']);
        }
        return ($a['sales'] > $b['sales']) ? -1 : 1;
    });

    $topProducts = [];
    foreach ($product as $productName => $agg) {
        // 商品別集計をJSON出力用の配列へ変換。
        $topProducts[] = [
            'product' => $productName,
            'sales' => money($agg['sales']),
            'quantity' => $agg['quantity'],
        ];
    }
    // 商品は売上降順で並べ、Top3のみ返す。
    usort($topProducts, function (array $a, array $b): int {
        if ($a['sales'] === $b['sales']) {
            return strcmp($a['product'], $b['product']);
        }
        return ($a['sales'] > $b['sales']) ? -1 : 1;
    });
    $topProducts = array_slice($topProducts, 0, 3);

    // 3言語で同じJSONキー構造に揃える。
    // 比較時にキー不一致で差分が出ないよう、命名も固定する。
    return [
        'daily_sales' => $dailySales,
        'category_sales' => $categorySales,
        'top_products' => $topProducts,
    ];
}

// CLIエントリーポイント:
// 引数を受け取り、集計実行後に整形JSONを標準出力へ出す。
$inputPath = parse_input_path($argv);
$result = aggregate_sales($inputPath);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
