<?php

// このファイルは「お題3: 複数データの結合（JOIN相当）」の PHP 実装。
// PHP では、顧客マスタを連想配列インデックス化し、注文を foreach で突合する。
// Python の pandas.merge と比較しながら、JOIN処理を明示的に学ぶ目的の実装。

/**
 * CLI引数から顧客CSV/注文CSVのパスを取得する。
 *
 * @param array<int, string> $argv CLI引数。
 *
 * @return array{customers: string, orders: string} 入力パス。
 */
function parse_paths(array $argv): array
{
    $customers = '';
    $orders = '';
    $debug = false;
    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--customers' && isset($argv[$i + 1])) {
            $customers = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--orders' && isset($argv[$i + 1])) {
            $orders = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--debug') {
            $debug = true;
        }
    }

    if ($customers === '' || $orders === '') {
        throw new InvalidArgumentException('必須引数がありません: --customers <csv_path> --orders <csv_path>');
    }

    return ['customers' => $customers, 'orders' => $orders, 'debug' => $debug];
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
 * ヘッダー付きCSVを読み込み、連想配列の行配列を返す。
 *
 * @param string $path CSVファイルパス。
 *
 * @return array<int, array<string, string>> 行配列。
 */
function read_csv(string $path): array
{
    // 学習用の最小CSV読み込み実装。
    // 実務では quoted field やエスケープを想定して専用ライブラリ検討が必要。
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
            $row[$name] = $line[$index] ?? '';
        }
        $rows[] = $row;
    }

    fclose($handle);
    return $rows;
}

/**
 * デバッグログを標準エラーへ出力する。
 *
 * @param bool $debug デバッグ有無。
 * @param string $title タイトル。
 * @param mixed $value 表示対象。
 *
 * @return void
 */
function debug_log(bool $debug, string $title, mixed $value): void
{
    if (!$debug) {
        return;
    }
    fwrite(STDERR, "DEBUG: {$title}\n");
    fwrite(STDERR, json_encode($value, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL);
}

/**
 * セグメント売上を作成する。
 *
 * @param array<int, array<string, mixed>> $rows JOIN済み行。
 *
 * @return array<int, array<string, int|float|string>> 集計結果。
 */
function build_segment_sales(array $rows): array
{
    // PHP は連想配列を集計バッファとして更新する。
    // Python の groupby/agg、Node.js の Map 更新と同じ役割。
    $segmentMap = [];

    foreach ($rows as $row) {
        $segment = (string)$row['segment'];
        if (!isset($segmentMap[$segment])) {
            $segmentMap[$segment] = [
                'total_sales' => 0.0,
                'order_count' => 0,
                'customer_ids' => [],
            ];
        }
        $segmentMap[$segment]['total_sales'] += (float)$row['line_total'];
        $segmentMap[$segment]['order_count'] += 1;
        $segmentMap[$segment]['customer_ids'][(string)$row['customer_id']] = true;
    }

    $result = [];
    foreach ($segmentMap as $segment => $agg) {
        $totalSales = (float)$agg['total_sales'];
        $orderCount = (int)$agg['order_count'];
        $result[] = [
            'segment' => $segment,
            'total_sales' => normalize_number($totalSales),
            'order_count' => $orderCount,
            'avg_order_amount' => normalize_number($totalSales / $orderCount),
            'unique_customers' => count($agg['customer_ids']),
        ];
    }

    usort($result, function (array $a, array $b): int {
        if ($a['total_sales'] === $b['total_sales']) {
            return strcmp((string)$a['segment'], (string)$b['segment']);
        }
        return ($a['total_sales'] > $b['total_sales']) ? -1 : 1;
    });

    return $result;
}

/**
 * 顧客CSVと注文CSVを突合し、INNER/LEFT JOIN相当の分析結果を返す。
 *
 * @param string $customersPath 顧客CSVパス。
 * @param string $ordersPath 注文CSVパス。
 *
 * @return array<string, mixed> 結果JSON。
 */
function join_and_analyze(string $customersPath, string $ordersPath, bool $debug = false): array
{
    // 行程1: 入力読み込み後、数量/単価を型変換して line_total を作る。
    // line_total を共通化しておくと、JOIN後の集計式を再利用しやすい。
    $customers = read_csv($customersPath);
    $ordersRaw = read_csv($ordersPath);

    $orders = [];
    foreach ($ordersRaw as $row) {
        $quantity = (int)$row['quantity'];
        $unitPrice = (float)$row['unit_price'];
        $row['quantity'] = $quantity;
        $row['unit_price'] = $unitPrice;
        $row['line_total'] = $quantity * $unitPrice;
        $orders[] = $row;
    }
    debug_log($debug, 'customers 読み込み直後', $customers);
    debug_log($debug, 'orders 読み込み直後(line_total込み)', $orders);

    // PHP での JOIN 表現:
    // 1) customer_id をキーに顧客マスタを索引化
    // 2) 注文を1件ずつ照合して INNER/LEFT 相当を作る
    // Python なら merge(on='customer_id', how='inner/left') で記述できる。
    $customerIndex = [];
    foreach ($customers as $customer) {
        $customerIndex[$customer['customer_id']] = $customer;
    }

    $innerJoined = [];
    $leftJoined = [];
    $orphanOrders = [];

    // 行程2: INNER/LEFT 相当の突合。
    // INNER: 顧客マスタに存在する注文のみ採用。
    // LEFT: 注文全件を採用し、未突合は Unknown セグメントへ寄せる。
    foreach ($orders as $order) {
        $customerId = (string)$order['customer_id'];
        if (isset($customerIndex[$customerId])) {
            $customer = $customerIndex[$customerId];
            $joined = $order;
            $joined['customer_name'] = $customer['customer_name'];
            $joined['segment'] = $customer['segment'];
            $innerJoined[] = $joined;
            $leftJoined[] = $joined;
        } else {
            $unknownJoined = $order;
            $unknownJoined['customer_name'] = null;
            $unknownJoined['segment'] = 'Unknown';
            $leftJoined[] = $unknownJoined;
            $orphanOrders[] = [
                'order_id' => $order['order_id'],
                'order_date' => $order['order_date'],
                'customer_id' => $order['customer_id'],
                'product' => $order['product'],
                'line_total' => normalize_number((float)$order['line_total']),
            ];
        }
    }
    debug_log($debug, 'innerJoined', $innerJoined);
    debug_log($debug, 'leftJoined', $leftJoined);
    debug_log($debug, 'orphanOrders', $orphanOrders);

    $segmentSalesInner = build_segment_sales($innerJoined);
    $segmentSalesLeft = build_segment_sales($leftJoined);
    debug_log($debug, 'segmentSalesInner', $segmentSalesInner);
    debug_log($debug, 'segmentSalesLeft', $segmentSalesLeft);

    // セグメント x 商品 の売上を作り、セグメントごとに売上トップ商品を1件採用する。
    $segmentProductMap = [];
    foreach ($innerJoined as $row) {
        $key = $row['segment'] . '::' . $row['product'];
        if (!isset($segmentProductMap[$key])) {
            $segmentProductMap[$key] = [
                'segment' => $row['segment'],
                'product' => $row['product'],
                'total_sales' => 0.0,
                'total_quantity' => 0,
            ];
        }
        $segmentProductMap[$key]['total_sales'] += (float)$row['line_total'];
        $segmentProductMap[$key]['total_quantity'] += (int)$row['quantity'];
    }

    $bySegment = [];
    foreach ($segmentProductMap as $item) {
        $segment = (string)$item['segment'];
        if (!isset($bySegment[$segment])) {
            $bySegment[$segment] = [];
        }
        $bySegment[$segment][] = $item;
    }

    $topProductsBySegmentInner = [];
    foreach ($bySegment as $segment => $items) {
        usort($items, function (array $a, array $b): int {
            if ($a['total_sales'] === $b['total_sales']) {
                return strcmp((string)$a['product'], (string)$b['product']);
            }
            return ($a['total_sales'] > $b['total_sales']) ? -1 : 1;
        });
        $top = $items[0];
        // セグメントごとのトップ商品を1件だけ採用する。
        // これは Python の drop_duplicates(subset=["segment"], keep="first") 相当。
        $topProductsBySegmentInner[] = [
            'segment' => $segment,
            'product' => $top['product'],
            'total_sales' => normalize_number((float)$top['total_sales']),
            'total_quantity' => (int)$top['total_quantity'],
        ];
    }
    debug_log($debug, 'topProductsBySegmentInner', $topProductsBySegmentInner);

    usort($topProductsBySegmentInner, function (array $a, array $b): int {
        if ($a['total_sales'] === $b['total_sales']) {
            return strcmp((string)$a['segment'], (string)$b['segment']);
        }
        return ($a['total_sales'] > $b['total_sales']) ? -1 : 1;
    });

    usort($orphanOrders, function (array $a, array $b): int {
        $dateCompare = strcmp((string)$a['order_date'], (string)$b['order_date']);
        if ($dateCompare !== 0) {
            return $dateCompare;
        }
        return strcmp((string)$a['order_id'], (string)$b['order_id']);
    });

    // 行程3: 出力JSONのキー構造を固定する。
    // 3言語で同一スキーマにすることで、自動diffで一致検証できる。
    return [
        // 集計処理全体の件数サマリ。
        'summary' => [
            // 顧客マスタCSVの総件数。
            'customers_count' => count($customers),
            // 注文CSVの総件数。
            'orders_count' => count($orders),
            // INNER JOINで突合できた注文件数。
            'inner_join_rows' => count($innerJoined),
            // LEFT JOINで保持した注文件数（orders総件数と同値）。
            'left_join_rows' => count($leftJoined),
            // 顧客マスタ未登録の注文件数。
            'orphan_order_count' => count($orphanOrders),
        ],
        // INNER JOINベースのセグメント売上集計。
        'segment_sales_inner' => $segmentSalesInner,
        // LEFT JOINベースのセグメント売上集計（Unknown含む）。
        'segment_sales_left' => $segmentSalesLeft,
        // INNER JOINデータから算出したセグメント別トップ商品。
        'top_products_by_segment_inner' => $topProductsBySegmentInner,
        // 顧客マスタ未登録注文の明細一覧。
        'orphan_orders' => $orphanOrders,
    ];
}

$paths = parse_paths($argv);
$result = join_and_analyze($paths['customers'], $paths['orders'], (bool)$paths['debug']);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
