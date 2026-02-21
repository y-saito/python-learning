<?php

// このファイルは「お題4: SQL連携と分析クエリ」の PHP 実装。
// PHP では PDO で SQL抽出し、foreach で再集計する。
// Node.js の pg、Python の pandas.read_sql_query と比較する学習用実装。

/**
 * CLI引数を解析する。
 *
 * @param array<int, string> $argv CLI引数。
 *
 * @return array{
 *   host: string,
 *   port: int,
 *   database: string,
 *   user: string,
 *   password: string,
 *   high_value_threshold: float,
 *   debug: bool
 * } 実行オプション。
 */
function parse_args(array $argv): array
{
    $options = [
        'host' => 'host.docker.internal',
        'port' => 5432,
        'database' => 'app',
        'user' => 'app',
        'password' => 'app',
        'high_value_threshold' => 500.0,
        'debug' => false,
    ];

    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--host' && isset($argv[$i + 1])) {
            $options['host'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--port' && isset($argv[$i + 1])) {
            $options['port'] = (int)$argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--database' && isset($argv[$i + 1])) {
            $options['database'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--user' && isset($argv[$i + 1])) {
            $options['user'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--password' && isset($argv[$i + 1])) {
            $options['password'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--high-value-threshold' && isset($argv[$i + 1])) {
            $options['high_value_threshold'] = (float)$argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--debug') {
            $options['debug'] = true;
        }
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
 * SQL抽出行を集計してお題4の固定JSONスキーマを返す。
 *
 * @param array<int, array<string, mixed>> $rows SQL抽出行。
 * @param float $highValueThreshold 高額注文しきい値。
 * @param bool $debug デバッグ有無。
 *
 * @return array<string, mixed> 集計結果。
 */
function aggregate_rows(array $rows, float $highValueThreshold, bool $debug): array
{
    if (count($rows) === 0) {
        throw new RuntimeException('sales_orders にデータがありません。先にシードSQLを投入してください。');
    }

    $dailyMap = [];
    $segmentMap = [];
    $paymentMap = [];
    $highValueOrders = [];
    $totalRevenue = 0.0;
    $dates = [];

    foreach ($rows as $row) {
        $orderDate = (string)$row['order_date'];
        $segment = (string)$row['customer_segment'];
        $paymentMethod = (string)$row['payment_method'];
        $orderAmount = (float)$row['order_amount'];

        $totalRevenue += $orderAmount;
        $dates[$orderDate] = true;

        if (!isset($dailyMap[$orderDate])) {
            $dailyMap[$orderDate] = 0.0;
        }
        $dailyMap[$orderDate] += $orderAmount;

        if (!isset($segmentMap[$segment])) {
            $segmentMap[$segment] = ['total_sales' => 0.0, 'order_count' => 0];
        }
        $segmentMap[$segment]['total_sales'] += $orderAmount;
        $segmentMap[$segment]['order_count'] += 1;

        if (!isset($paymentMap[$paymentMethod])) {
            $paymentMap[$paymentMethod] = ['total_sales' => 0.0, 'order_count' => 0];
        }
        $paymentMap[$paymentMethod]['total_sales'] += $orderAmount;
        $paymentMap[$paymentMethod]['order_count'] += 1;

        if ($orderAmount >= $highValueThreshold) {
            $highValueOrders[] = [
                // 注文ID。
                'order_id' => (string)$row['order_id'],
                // 注文日。
                'order_date' => $orderDate,
                // 注文顧客のセグメント。
                'segment' => $segment,
                // 注文時の決済手段。
                'payment_method' => $paymentMethod,
                // 注文金額（しきい値以上）。
                'order_amount' => normalize_number($orderAmount),
            ];
        }
    }

    $dailySales = [];
    foreach ($dailyMap as $date => $sales) {
        $dailySales[] = [
            // 売上日。
            'date' => $date,
            // その日の売上合計。
            'sales' => normalize_number((float)$sales),
        ];
    }
    usort($dailySales, fn (array $a, array $b): int => strcmp((string)$a['date'], (string)$b['date']));

    $segmentSales = [];
    foreach ($segmentMap as $segment => $agg) {
        $segmentSales[] = [
            // 顧客セグメント名。
            'segment' => $segment,
            // セグメント売上合計。
            'total_sales' => normalize_number((float)$agg['total_sales']),
            // セグメント内の注文件数。
            'order_count' => (int)$agg['order_count'],
            // セグメント内の平均注文金額。
            'avg_order_amount' => normalize_number((float)$agg['total_sales'] / (int)$agg['order_count']),
        ];
    }
    usort($segmentSales, function (array $a, array $b): int {
        if ($a['total_sales'] === $b['total_sales']) {
            return strcmp((string)$a['segment'], (string)$b['segment']);
        }
        return ($a['total_sales'] > $b['total_sales']) ? -1 : 1;
    });

    $paymentMethodSales = [];
    foreach ($paymentMap as $paymentMethod => $agg) {
        $paymentMethodSales[] = [
            // 決済手段名。
            'payment_method' => $paymentMethod,
            // 決済手段ごとの売上合計。
            'total_sales' => normalize_number((float)$agg['total_sales']),
            // 決済手段ごとの注文件数。
            'order_count' => (int)$agg['order_count'],
            // 決済手段ごとの平均注文金額。
            'avg_order_amount' => normalize_number((float)$agg['total_sales'] / (int)$agg['order_count']),
        ];
    }
    usort($paymentMethodSales, function (array $a, array $b): int {
        if ($a['total_sales'] === $b['total_sales']) {
            return strcmp((string)$a['payment_method'], (string)$b['payment_method']);
        }
        return ($a['total_sales'] > $b['total_sales']) ? -1 : 1;
    });

    usort($highValueOrders, function (array $a, array $b): int {
        if ($a['order_amount'] === $b['order_amount']) {
            $idCompare = strcmp((string)$a['order_id'], (string)$b['order_id']);
            if ($idCompare !== 0) {
                return $idCompare;
            }
            return strcmp((string)$a['order_date'], (string)$b['order_date']);
        }
        return ($a['order_amount'] > $b['order_amount']) ? -1 : 1;
    });

    $sortedDates = array_keys($dates);
    sort($sortedDates);

    debug_log($debug, 'dailySales', $dailySales);
    debug_log($debug, 'segmentSales', $segmentSales);
    debug_log($debug, 'paymentMethodSales', $paymentMethodSales);
    debug_log($debug, 'highValueOrders', $highValueOrders);

    return [
        // レコード件数・期間・総売上の要約。
        'summary' => [
            // SQL抽出後の総件数。
            'total_rows' => count($rows),
            // 集計期間の開始日。
            'date_range_start' => $sortedDates[0],
            // 集計期間の終了日。
            'date_range_end' => $sortedDates[count($sortedDates) - 1],
            // 全注文の売上合計。
            'total_revenue' => normalize_number($totalRevenue),
            // 高額注文（しきい値以上）の件数。
            'high_value_order_count' => count($highValueOrders),
        ],
        // 日次売上配列。
        'daily_sales' => $dailySales,
        // セグメント別売上配列。
        'segment_sales' => $segmentSales,
        // 決済手段別売上配列。
        'payment_method_sales' => $paymentMethodSales,
        // 高額注文の明細配列。
        'high_value_orders' => $highValueOrders,
    ];
}

/**
 * PostgreSQL から明細を抽出し、再集計結果を出力する。
 *
 * @param array{
 *   host: string,
 *   port: int,
 *   database: string,
 *   user: string,
 *   password: string,
 *   high_value_threshold: float,
 *   debug: bool
 * } $options 実行オプション。
 *
 * @return array<string, mixed> 集計結果。
 */
function build_sql_sales_report(array $options): array
{
    // PHP:
    //   PDO で SQL抽出 -> 配列を foreach 集計するのが基本パターン。
    // Python では pandas.read_sql_query で DataFrame 化、
    // Node.js では pg 取得後に Map 集計、という流れと対応付ける。
    $dsn = sprintf(
        'pgsql:host=%s;port=%d;dbname=%s',
        $options['host'],
        $options['port'],
        $options['database']
    );
    $pdo = new PDO($dsn, $options['user'], $options['password']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $sql = <<<SQL
SELECT
  order_id,
  order_date::text AS order_date,
  customer_segment,
  payment_method,
  order_amount
FROM sales_orders
ORDER BY order_date, order_id
SQL;

    $statement = $pdo->query($sql);
    if ($statement === false) {
        throw new RuntimeException('sales_orders のSELECTに失敗しました。');
    }
    $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
    debug_log($options['debug'], 'SQL抽出直後のrows', $rows);

    return aggregate_rows($rows, $options['high_value_threshold'], $options['debug']);
}

$options = parse_args($argv);
$result = build_sql_sales_report($options);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
