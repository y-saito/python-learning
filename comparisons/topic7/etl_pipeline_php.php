<?php

$autoloadPath = getenv('PARQUET_VENDOR_AUTOLOAD');
if ($autoloadPath === false || $autoloadPath === '') {
    $autoloadPath = __DIR__ . '/vendor/autoload.php';
}
if (!file_exists($autoloadPath)) {
    throw new RuntimeException("autoload.php が見つかりません: {$autoloadPath}");
}
require_once $autoloadPath;

use codename\parquet\ParquetExtensions;
use codename\parquet\CompressionMethod;
use codename\parquet\ParquetOptions;
use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;

// このファイルは「お題7: ETLミニパイプライン」の PHP 実装。
// PHP では foreach と連想配列で ETL を明示的に組み立てる。
// Python(pandas) と同じ段階/同じ出力スキーマに揃えて比較する。

/**
 * CLI引数を解析する。
 *
 * @param array<int, string> $argv CLI引数。
 *
 * @return array{input: string, output: string, debug: bool} 実行オプション。
 */
function parse_args(array $argv): array
{
    $options = [
        'input' => '',
        'output' => '',
        'debug' => false,
    ];

    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--input' && isset($argv[$i + 1])) {
            $options['input'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--output' && isset($argv[$i + 1])) {
            $options['output'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--debug') {
            $options['debug'] = true;
        }
    }

    if ($options['input'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --input <csv_path>');
    }
    if ($options['output'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --output <parquet_path>');
    }

    return $options;
}

/**
 * 比較しやすいJSON表現へ数値を正規化する。
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
 * ヘッダー付きCSVを読み込む。
 *
 * @param string $path CSVパス。
 *
 * @return array<int, array<string, string>> 行配列。
 */
function read_csv(string $path): array
{
    $handle = fopen($path, 'r');
    if ($handle === false) {
        throw new RuntimeException("入力ファイルを開けません: {$path}");
    }

    $header = fgetcsv($handle);
    if ($header === false) {
        fclose($handle);
        throw new RuntimeException("CSVヘッダーが見つかりません: {$path}");
    }

    $rows = [];
    while (($line = fgetcsv($handle)) !== false) {
        $row = [];
        foreach ($header as $index => $name) {
            $row[$name] = isset($line[$index]) ? trim((string)$line[$index]) : '';
        }
        $rows[] = $row;
    }

    fclose($handle);
    return $rows;
}

/**
 * 配列中央値を求める。
 *
 * @param array<int, float> $values 値配列。
 * @param float $fallback 空配列時の代替値。
 *
 * @return float 中央値。
 */
function median(array $values, float $fallback): float
{
    if (count($values) === 0) {
        return $fallback;
    }

    sort($values, SORT_NUMERIC);
    $count = count($values);
    $mid = intdiv($count, 2);

    if ($count % 2 === 0) {
        return ((float)$values[$mid - 1] + (float)$values[$mid]) / 2;
    }

    return (float)$values[$mid];
}

/**
 * 日付文字列を YYYY-MM-DD へ正規化する。
 *
 * @param string $value 日付文字列。
 *
 * @return string|null 正規化日付。失敗時は null。
 */
function normalize_date(string $value): ?string
{
    $ts = strtotime($value);
    if ($ts === false) {
        return null;
    }

    return gmdate('Y-m-d', $ts);
}

/**
 * Transform: 欠損補完・型変換・派生列追加を行う。
 *
 * @param array<int, array<string, string>> $rawRows 生注文行。
 *
 * @return array{cleaned_rows: array<int, array<string, mixed>>, transform_stats: array<string, int|float>} 変換結果。
 */
function transform_orders(array $rawRows): array
{
    // 行程1: 日付パースで不正日付を除外。
    // Python は pandas.to_datetime(errors='coerce') で列単位判定が可能。
    // PHP/Node.js は行ごとに parse 成否を確認する実装になる。
    $rowsWithDate = [];
    $droppedInvalidOrderDateCount = 0;
    foreach ($rawRows as $row) {
        $orderDate = normalize_date((string)($row['order_date'] ?? ''));
        if ($orderDate === null) {
            $droppedInvalidOrderDateCount++;
            continue;
        }

        $rowsWithDate[] = [
            'order_id' => (string)($row['order_id'] ?? ''),
            'order_date' => $orderDate,
            'customer_id' => (string)($row['customer_id'] ?? ''),
            'product' => (string)($row['product'] ?? ''),
            'quantity_raw' => (string)($row['quantity'] ?? ''),
            'unit_price_raw' => (string)($row['unit_price'] ?? ''),
        ];
    }

    // 行程2: 補完値（中央値）を算出。
    $quantityCandidates = [];
    $unitPriceCandidates = [];
    foreach ($rowsWithDate as $row) {
        if (is_numeric($row['quantity_raw'])) {
            $quantity = (float)$row['quantity_raw'];
            if ($quantity > 0) {
                $quantityCandidates[] = $quantity;
            }
        }

        if (is_numeric($row['unit_price_raw'])) {
            $unitPrice = (float)$row['unit_price_raw'];
            if ($unitPrice >= 0) {
                $unitPriceCandidates[] = $unitPrice;
            }
        }
    }

    $quantityFillValue = median($quantityCandidates, 1.0);
    $unitPriceFillValue = median($unitPriceCandidates, 0.0);

    // 行程3: 欠損補完と派生列作成。
    $filledCustomerIdCount = 0;
    $filledQuantityCount = 0;
    $filledUnitPriceCount = 0;
    $cleanedRows = [];

    foreach ($rowsWithDate as $row) {
        $customerId = trim($row['customer_id']);
        if ($customerId === '') {
            $customerId = 'UNKNOWN_CUSTOMER';
            $filledCustomerIdCount++;
        }

        $quantity = is_numeric($row['quantity_raw']) ? (float)$row['quantity_raw'] : NAN;
        if (!is_finite($quantity) || $quantity <= 0) {
            $quantity = $quantityFillValue;
            $filledQuantityCount++;
        }

        $unitPrice = is_numeric($row['unit_price_raw']) ? (float)$row['unit_price_raw'] : NAN;
        if (!is_finite($unitPrice) || $unitPrice < 0) {
            $unitPrice = $unitPriceFillValue;
            $filledUnitPriceCount++;
        }

        $roundedQuantity = (int)round($quantity);
        $lineTotal = round($roundedQuantity * $unitPrice, 2);

        $cleanedRows[] = [
            'order_id' => $row['order_id'],
            'order_date' => $row['order_date'],
            'customer_id' => $customerId,
            'product' => $row['product'],
            'quantity' => $roundedQuantity,
            'unit_price' => (float)$unitPrice,
            'order_month' => substr($row['order_date'], 0, 7),
            'line_total' => $lineTotal,
        ];
    }

    usort($cleanedRows, function (array $a, array $b): int {
        $dateCompare = strcmp((string)$a['order_date'], (string)$b['order_date']);
        if ($dateCompare !== 0) {
            return $dateCompare;
        }
        return strcmp((string)$a['order_id'], (string)$b['order_id']);
    });

    return [
        'cleaned_rows' => $cleanedRows,
        'transform_stats' => [
            'transformed_records' => count($cleanedRows),
            'dropped_invalid_order_date_count' => $droppedInvalidOrderDateCount,
            'filled_customer_id_count' => $filledCustomerIdCount,
            'filled_quantity_count' => $filledQuantityCount,
            'filled_unit_price_count' => $filledUnitPriceCount,
            'quantity_fill_value' => normalize_number($quantityFillValue),
            'unit_price_fill_value' => normalize_number($unitPriceFillValue),
        ],
    ];
}

/**
 * Load: 整形済み行を Parquet へ保存する。
 *
 * @param array<int, array<string, mixed>> $cleanedRows 整形済み行。
 * @param string $outputPath 出力Parquetパス。
 *
 * @return array{output_path: string, loaded_records: int} 保存統計。
 */
function load_parquet(array $cleanedRows, string $outputPath): array
{
    // PHP では Parquet スキーマと列データを明示して書き込む。
    // Python の to_parquet と比較すると、Load工程のコード量が増える。
    $directory = dirname($outputPath);
    if (!is_dir($directory)) {
        mkdir($directory, 0777, true);
    }

    $schema = new Schema([
        DataField::createFromType('order_id', 'string'),
        DataField::createFromType('order_date', 'string'),
        DataField::createFromType('customer_id', 'string'),
        DataField::createFromType('product', 'string'),
        DataField::createFromType('quantity', 'integer'),
        DataField::createFromType('unit_price', 'double'),
        DataField::createFromType('order_month', 'string'),
        DataField::createFromType('line_total', 'double'),
    ]);

    $columns = [
        'order_id' => [],
        'order_date' => [],
        'customer_id' => [],
        'product' => [],
        'quantity' => [],
        'unit_price' => [],
        'order_month' => [],
        'line_total' => [],
    ];

    foreach ($cleanedRows as $row) {
        $columns['order_id'][] = (string)$row['order_id'];
        $columns['order_date'][] = (string)$row['order_date'];
        $columns['customer_id'][] = (string)$row['customer_id'];
        $columns['product'][] = (string)$row['product'];
        $columns['quantity'][] = (int)$row['quantity'];
        $columns['unit_price'][] = (float)$row['unit_price'];
        $columns['order_month'][] = (string)$row['order_month'];
        $columns['line_total'][] = (float)$row['line_total'];
    }

    $dataColumns = [
        new DataColumn(DataField::createFromType('order_id', 'string'), $columns['order_id']),
        new DataColumn(DataField::createFromType('order_date', 'string'), $columns['order_date']),
        new DataColumn(DataField::createFromType('customer_id', 'string'), $columns['customer_id']),
        new DataColumn(DataField::createFromType('product', 'string'), $columns['product']),
        new DataColumn(DataField::createFromType('quantity', 'integer'), $columns['quantity']),
        new DataColumn(DataField::createFromType('unit_price', 'double'), $columns['unit_price']),
        new DataColumn(DataField::createFromType('order_month', 'string'), $columns['order_month']),
        new DataColumn(DataField::createFromType('line_total', 'double'), $columns['line_total']),
    ];

    $stream = fopen($outputPath, 'w+');
    if ($stream === false) {
        throw new RuntimeException("出力ファイルを開けません: {$outputPath}");
    }

    $options = new ParquetOptions();
    $options->TreatByteArrayAsString = true;

    // 圧縮方式をNoneにして、学習環境の追加拡張依存を避ける。
    ParquetExtensions::WriteSingleRowGroupParquetFile($stream, $schema, $dataColumns, $options);
    fclose($stream);

    $normalizedOutputPath = str_starts_with($outputPath, '/workspace/')
        ? substr($outputPath, strlen('/workspace/'))
        : $outputPath;

    return [
        'output_path' => $normalizedOutputPath,
        'loaded_records' => count($cleanedRows),
    ];
}

$options = parse_args($argv);
$rawRows = read_csv($options['input']);
$extractStats = ['input_records' => count($rawRows)];

$transformed = transform_orders($rawRows);
$cleanedRows = $transformed['cleaned_rows'];
$transformStats = $transformed['transform_stats'];
$loadStats = load_parquet($cleanedRows, $options['output']);

if ($options['debug']) {
    fwrite(STDERR, '[debug] raw_rows=' . json_encode($rawRows, JSON_UNESCAPED_UNICODE) . PHP_EOL);
    fwrite(STDERR, '[debug] cleaned_rows=' . json_encode($cleanedRows, JSON_UNESCAPED_UNICODE) . PHP_EOL);
}

$totalSales = 0.0;
foreach ($cleanedRows as $row) {
    $totalSales += (float)$row['line_total'];
}

$sampleRows = [];
foreach (array_slice($cleanedRows, 0, 3) as $row) {
    $sampleRows[] = [
        'order_id' => (string)$row['order_id'],
        'order_date' => (string)$row['order_date'],
        'customer_id' => (string)$row['customer_id'],
        'product' => (string)$row['product'],
        'quantity' => (int)$row['quantity'],
        'unit_price' => normalize_number((float)$row['unit_price']),
        'order_month' => (string)$row['order_month'],
        'line_total' => normalize_number((float)$row['line_total']),
    ];
}

$result = [
    'summary' => [
        'extract' => $extractStats,
        'transform' => $transformStats,
        'load' => $loadStats,
        'total_sales' => normalize_number($totalSales),
    ],
    'sample_cleaned_rows' => $sampleRows,
];

echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
