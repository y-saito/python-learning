<?php

$autoloadPath = getenv('PARQUET_VENDOR_AUTOLOAD');
if ($autoloadPath === false || $autoloadPath === '') {
    $autoloadPath = __DIR__ . '/vendor/autoload.php';
}
if (!file_exists($autoloadPath)) {
    throw new RuntimeException("autoload.php が見つかりません: {$autoloadPath}");
}
require_once $autoloadPath;

use codename\parquet\helper\ParquetDataIterator;

// このファイルは「お題6: JSON / Parquet 読み込み比較」の PHP 実装。
// PHP では JSON は json_decode で扱いやすいが、Parquet は追加ライブラリ(codename/parquet)が必要。
// Python(pandas)・Node.js(parquetjs-lite)と同じ出力スキーマで比較する。

/**
 * CLI引数を解析する。
 *
 * @param array<int, string> $argv CLI引数。
 *
 * @return array{json_input: string, parquet_input: string, debug: bool} 実行オプション。
 */
function parse_args(array $argv): array
{
    $options = [
        'json_input' => '',
        'parquet_input' => '',
        'debug' => false,
    ];

    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--json-input' && isset($argv[$i + 1])) {
            $options['json_input'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--parquet-input' && isset($argv[$i + 1])) {
            $options['parquet_input'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--debug') {
            $options['debug'] = true;
        }
    }

    if ($options['json_input'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --json-input <path>');
    }
    if ($options['parquet_input'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --parquet-input <path>');
    }

    return $options;
}

/**
 * 数値を比較しやすいJSON表現へ正規化する。
 *
 * @param float $value 正規化前の値。
 *
 * @return float|int 正規化後の値。
 */
function normalize_number(float $value): float|int
{
    $rounded = round($value, 2);
    if (fmod($rounded, 1.0) === 0.0) {
        return (int)$rounded;
    }
    return $rounded;
}

/**
 * JSONファイルから売上明細配列を読み込む。
 *
 * @param string $path JSONファイルパス。
 *
 * @return array<int, array{date: string, product: string, category: string, quantity: int, price: float}> 売上明細。
 */
function read_json_sales(string $path): array
{
    // PHP: json_decode で配列に展開。
    // Node.js の JSON.parse、Python の pandas.read_json と同じ役割。
    $decoded = json_decode((string)file_get_contents($path), true, flags: JSON_THROW_ON_ERROR);
    if (!is_array($decoded)) {
        throw new RuntimeException('JSONの形式が不正です。');
    }

    $rows = [];
    foreach ($decoded as $row) {
        if (!is_array($row)) {
            continue;
        }
        $rows[] = [
            'date' => (string)($row['date'] ?? ''),
            'product' => (string)($row['product'] ?? ''),
            'category' => (string)($row['category'] ?? ''),
            'quantity' => (int)($row['quantity'] ?? 0),
            'price' => (float)($row['price'] ?? 0.0),
        ];
    }

    return $rows;
}

/**
 * Parquetファイルから売上明細配列を読み込む。
 *
 * @param string $path Parquetファイルパス。
 *
 * @return array<int, array{date: string, product: string, category: string, quantity: int, price: float}> 売上明細。
 */
function read_parquet_sales(string $path): array
{
    // PHP: Parquet は codename/parquet で読み込み。
    // Python は pandas.read_parquet で統一的に扱える点が比較ポイント。
    $rows = [];
    $iterator = ParquetDataIterator::fromFile($path);
    foreach ($iterator as $row) {
        if (!is_array($row)) {
            continue;
        }
        $rows[] = [
            'date' => (string)($row['date'] ?? ''),
            'product' => (string)($row['product'] ?? ''),
            'category' => (string)($row['category'] ?? ''),
            'quantity' => (int)($row['quantity'] ?? 0),
            'price' => (float)($row['price'] ?? 0.0),
        ];
    }
    return $rows;
}

/**
 * 売上明細を日次・カテゴリ・商品Top3で集計する。
 *
 * @param array<int, array{date: string, product: string, category: string, quantity: int, price: float}> $rows 売上明細。
 *
 * @return array{
 *   daily_sales: array<int, array{date: string, sales: float|int}>,
 *   category_sales: array<int, array{category: string, sales: float|int}>,
 *   top_products: array<int, array{product: string, sales: float|int, quantity: int}>
 * } 集計結果。
 */
function aggregate_sales(array $rows): array
{
    // PHP/Node.js は連想配列/Mapを更新して集計。
    // Python は groupby/agg を使って短く宣言できる。
    $dailyMap = [];
    $categoryMap = [];
    $productMap = [];

    foreach ($rows as $row) {
        $lineTotal = $row['quantity'] * $row['price'];

        if (!isset($dailyMap[$row['date']])) {
            $dailyMap[$row['date']] = 0.0;
        }
        if (!isset($categoryMap[$row['category']])) {
            $categoryMap[$row['category']] = 0.0;
        }
        if (!isset($productMap[$row['product']])) {
            $productMap[$row['product']] = ['sales' => 0.0, 'quantity' => 0];
        }

        $dailyMap[$row['date']] += $lineTotal;
        $categoryMap[$row['category']] += $lineTotal;
        $productMap[$row['product']]['sales'] += $lineTotal;
        $productMap[$row['product']]['quantity'] += $row['quantity'];
    }

    ksort($dailyMap);
    $dailySales = [];
    foreach ($dailyMap as $date => $sales) {
        $dailySales[] = [
            'date' => $date,
            'sales' => normalize_number((float)$sales),
        ];
    }

    $categorySales = [];
    foreach ($categoryMap as $category => $sales) {
        $categorySales[] = [
            'category' => $category,
            'sales' => normalize_number((float)$sales),
        ];
    }
    usort($categorySales, function (array $a, array $b): int {
        if ($a['sales'] === $b['sales']) {
            return strcmp((string)$a['category'], (string)$b['category']);
        }
        return ($a['sales'] > $b['sales']) ? -1 : 1;
    });

    $topProducts = [];
    foreach ($productMap as $product => $agg) {
        $topProducts[] = [
            'product' => (string)$product,
            'sales' => normalize_number((float)$agg['sales']),
            'quantity' => (int)$agg['quantity'],
        ];
    }
    usort($topProducts, function (array $a, array $b): int {
        if ($a['sales'] === $b['sales']) {
            return strcmp((string)$a['product'], (string)$b['product']);
        }
        return ($a['sales'] > $b['sales']) ? -1 : 1;
    });
    $topProducts = array_slice($topProducts, 0, 3);

    return [
        'daily_sales' => $dailySales,
        'category_sales' => $categorySales,
        'top_products' => $topProducts,
    ];
}

/**
 * 2配列をインデックス単位で比較して差分のみ返す。
 *
 * @param array<int, array<string, mixed>> $jsonItems JSON集計配列。
 * @param array<int, array<string, mixed>> $parquetItems Parquet集計配列。
 *
 * @return array<int, array{index: int, json_value: array<string, mixed>|null, parquet_value: array<string, mixed>|null}> 差分配列。
 */
function compare_items(array $jsonItems, array $parquetItems): array
{
    $differences = [];
    $maxLen = max(count($jsonItems), count($parquetItems));

    for ($i = 0; $i < $maxLen; $i++) {
        $jsonValue = $jsonItems[$i] ?? null;
        $parquetValue = $parquetItems[$i] ?? null;
        if ($jsonValue !== $parquetValue) {
            $differences[] = [
                'index' => $i,
                'json_value' => $jsonValue,
                'parquet_value' => $parquetValue,
            ];
        }
    }

    return $differences;
}

/**
 * 比較結果JSONを組み立てる。
 *
 * @param array<int, array{date: string, product: string, category: string, quantity: int, price: float}> $jsonRows JSON明細。
 * @param array<int, array{date: string, product: string, category: string, quantity: int, price: float}> $parquetRows Parquet明細。
 *
 * @return array<string, mixed> 結果JSON。
 */
function build_result(array $jsonRows, array $parquetRows): array
{
    $jsonAggregations = aggregate_sales($jsonRows);
    $parquetAggregations = aggregate_sales($parquetRows);

    $differences = [
        'daily_sales' => compare_items($jsonAggregations['daily_sales'], $parquetAggregations['daily_sales']),
        'category_sales' => compare_items($jsonAggregations['category_sales'], $parquetAggregations['category_sales']),
        'top_products' => compare_items($jsonAggregations['top_products'], $parquetAggregations['top_products']),
    ];

    $isEquivalent = count($differences['daily_sales']) === 0
        && count($differences['category_sales']) === 0
        && count($differences['top_products']) === 0;

    return [
        'summary' => [
            // JSON入力の件数。
            'json_record_count' => count($jsonRows),
            // Parquet入力の件数。
            'parquet_record_count' => count($parquetRows),
            // 集計差分が全軸で0件か。
            'is_equivalent' => $isEquivalent,
        ],
        'json_aggregations' => $jsonAggregations,
        'parquet_aggregations' => $parquetAggregations,
        'differences' => $differences,
    ];
}

$options = parse_args($argv);
$jsonRows = read_json_sales($options['json_input']);
$parquetRows = read_parquet_sales($options['parquet_input']);

if ($options['debug']) {
    fwrite(STDERR, '[debug] json_rows=' . json_encode($jsonRows, JSON_UNESCAPED_UNICODE) . PHP_EOL);
    fwrite(STDERR, '[debug] parquet_rows=' . json_encode($parquetRows, JSON_UNESCAPED_UNICODE) . PHP_EOL);
}

$result = build_result($jsonRows, $parquetRows);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
