<?php

// このファイルは「お題5: 可視化によるレポート化」の PHP 実装。
// PHP では配列処理 + SVG手組みで可視化を行い、
// Python(matplotlib)・Node.js(手続きSVG)との記述差を比較できる形にする。

/**
 * CLI引数を解析する。
 *
 * @param array<int, string> $argv CLI引数。
 *
 * @return array{input: string, output_dir: string} 実行オプション。
 */
function parse_args(array $argv): array
{
    $options = [
        'input' => '',
        'output_dir' => '',
    ];

    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--input' && isset($argv[$i + 1])) {
            $options['input'] = $argv[$i + 1];
            $i++;
        } elseif ($argv[$i] === '--output-dir' && isset($argv[$i + 1])) {
            $options['output_dir'] = $argv[$i + 1];
            $i++;
        }
    }

    if ($options['input'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --input <csv_path>');
    }
    if ($options['output_dir'] === '') {
        throw new InvalidArgumentException('必須引数がありません: --output-dir <dir_path>');
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
 * CSVを読み込み、売上明細配列へ変換する。
 *
 * @param string $inputPath CSVパス。
 *
 * @return array<int, array{date: string, product: string, category: string, quantity: int, price: float, line_total: float}> 明細配列。
 */
function read_sales_csv(string $inputPath): array
{
    $handle = fopen($inputPath, 'r');
    if ($handle === false) {
        throw new RuntimeException("CSVを開けませんでした: {$inputPath}");
    }

    $rows = [];
    $headerSkipped = false;
    while (($line = fgetcsv($handle)) !== false) {
        if (!$headerSkipped) {
            $headerSkipped = true;
            continue;
        }
        $date = (string)$line[0];
        $product = (string)$line[1];
        $category = (string)$line[2];
        $quantity = (int)$line[3];
        $price = (float)$line[4];
        $rows[] = [
            'date' => $date,
            'product' => $product,
            'category' => $category,
            'quantity' => $quantity,
            'price' => $price,
            'line_total' => $quantity * $price,
        ];
    }
    fclose($handle);
    return $rows;
}

/**
 * 単純な縦棒グラフSVGを作成する。
 *
 * @param string $title グラフタイトル。
 * @param array<int, array{label: string, value: float}> $points 描画データ。
 *
 * @return string SVG文字列。
 */
function build_bar_svg(string $title, array $points): string
{
    $width = 720;
    $height = 420;
    $marginLeft = 70;
    $marginTop = 50;
    $plotW = 620;
    $plotH = 300;

    $maxValue = 1.0;
    foreach ($points as $point) {
        if ($point['value'] > $maxValue) {
            $maxValue = $point['value'];
        }
    }
    $barW = $plotW / max(count($points), 1);

    $bars = '';
    $labels = '';
    foreach ($points as $i => $point) {
        $barHeight = ($point['value'] / $maxValue) * $plotH;
        $x = $marginLeft + $i * $barW + 8;
        $y = $marginTop + ($plotH - $barHeight);
        $w = max($barW - 16, 8);
        $bars .= "<rect x=\"{$x}\" y=\"{$y}\" width=\"{$w}\" height=\"{$barHeight}\" fill=\"#3f7cac\" />";
        $labelX = $marginLeft + $i * $barW + $barW / 2;
        $labels .= "<text x=\"{$labelX}\" y=\"395\" text-anchor=\"middle\" font-size=\"11\">{$point['label']}</text>";
    }

    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        . "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{$width}\" height=\"{$height}\">\n"
        . "  <rect x=\"0\" y=\"0\" width=\"{$width}\" height=\"{$height}\" fill=\"#ffffff\"/>\n"
        . "  <text x=\"360\" y=\"28\" text-anchor=\"middle\" font-size=\"18\">{$title}</text>\n"
        . "  <line x1=\"{$marginLeft}\" y1=\"{$marginTop}\" x2=\"{$marginLeft}\" y2=\"350\" stroke=\"#444\"/>\n"
        . "  <line x1=\"{$marginLeft}\" y1=\"350\" x2=\"690\" y2=\"350\" stroke=\"#444\"/>\n"
        . "  {$bars}\n"
        . "  {$labels}\n"
        . "</svg>\n";
}

/**
 * 折れ線グラフSVGを作成する。
 *
 * @param string $title グラフタイトル。
 * @param array<int, array{label: string, value: float}> $points 描画データ。
 *
 * @return string SVG文字列。
 */
function build_line_svg(string $title, array $points): string
{
    $width = 720;
    $height = 420;
    $marginLeft = 70;
    $marginTop = 50;
    $plotW = 620;
    $plotH = 300;

    $maxValue = 1.0;
    foreach ($points as $point) {
        if ($point['value'] > $maxValue) {
            $maxValue = $point['value'];
        }
    }
    $stepX = count($points) > 1 ? $plotW / (count($points) - 1) : $plotW;

    $polylinePoints = [];
    $labels = '';
    foreach ($points as $i => $point) {
        $x = $marginLeft + $i * $stepX;
        $y = $marginTop + $plotH - ($point['value'] / $maxValue) * $plotH;
        $polylinePoints[] = "{$x},{$y}";
        $labels .= "<text x=\"{$x}\" y=\"395\" text-anchor=\"middle\" font-size=\"11\">{$point['label']}</text>";
    }
    $polyline = implode(' ', $polylinePoints);

    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        . "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{$width}\" height=\"{$height}\">\n"
        . "  <rect x=\"0\" y=\"0\" width=\"{$width}\" height=\"{$height}\" fill=\"#ffffff\"/>\n"
        . "  <text x=\"360\" y=\"28\" text-anchor=\"middle\" font-size=\"18\">{$title}</text>\n"
        . "  <line x1=\"{$marginLeft}\" y1=\"{$marginTop}\" x2=\"{$marginLeft}\" y2=\"350\" stroke=\"#444\"/>\n"
        . "  <line x1=\"{$marginLeft}\" y1=\"350\" x2=\"690\" y2=\"350\" stroke=\"#444\"/>\n"
        . "  <polyline fill=\"none\" stroke=\"#2a9d8f\" stroke-width=\"3\" points=\"{$polyline}\"/>\n"
        . "  {$labels}\n"
        . "</svg>\n";
}

/**
 * 意思決定向けの短い示唆を作成する。
 *
 * @param array<string, int|float|string> $summary サマリ。
 * @param array<int, array{category: string, sales: float|int}> $categorySales カテゴリ集計。
 * @param array<int, array{product: string, sales: float|int, quantity: int}> $topProducts 商品集計。
 *
 * @return array<int, string> 示唆配列。
 */
function build_insights(array $summary, array $categorySales, array $topProducts): array
{
    return [
        "売上最大日は {$summary['best_sales_day']} で、日次売上は {$summary['best_sales_amount']} です。",
        "カテゴリ別では {$categorySales[0]['category']} が最大で、売上は {$categorySales[0]['sales']} です。",
        "商品別では {$topProducts[0]['product']} が最大で、売上は {$topProducts[0]['sales']} です。",
    ];
}

/**
 * Markdownレポートを出力する。
 *
 * @param string $reportPath レポートパス。
 * @param array<string, int|float|string> $summary サマリ。
 * @param array<int, string> $insights 示唆配列。
 * @param array<string, string> $artifacts 生成物情報。
 *
 * @return void
 */
function write_markdown_report(string $reportPath, array $summary, array $insights, array $artifacts): void
{
    $lines = [
        '# お題5 可視化レポート',
        '',
        '## サマリ',
        "- 総注文件数: {$summary['total_orders']}",
        "- 全売上: {$summary['total_revenue']}",
        "- 注文平均売上: {$summary['average_order_value']}",
        "- 売上最大日: {$summary['best_sales_day']} ({$summary['best_sales_amount']})",
        '',
        '## 意思決定メモ',
    ];
    foreach ($insights as $insight) {
        $lines[] = "- {$insight}";
    }
    $lines[] = '';
    $lines[] = '## 生成グラフ';
    $lines[] = "- 日次売上: `{$artifacts['daily_sales_chart']}`";
    $lines[] = "- カテゴリ売上: `{$artifacts['category_sales_chart']}`";
    $lines[] = "- 上位商品: `{$artifacts['top_products_chart']}`";
    $lines[] = '';
    file_put_contents($reportPath, implode(PHP_EOL, $lines));
}

/**
 * 可視化・集計・レポートを実行して結果JSONを返す。
 *
 * @param string $inputPath 入力CSV。
 * @param string $outputDir 出力ディレクトリ。
 *
 * @return array<string, mixed> 結果JSON。
 */
function build_visual_sales_report(string $inputPath, string $outputDir): array
{
    $rows = read_sales_csv($inputPath);
    if (!is_dir($outputDir)) {
        mkdir($outputDir, 0777, true);
    }

    $dailyMap = [];
    $categoryMap = [];
    $productMap = [];
    foreach ($rows as $row) {
        $date = $row['date'];
        $category = $row['category'];
        $product = $row['product'];
        $lineTotal = $row['line_total'];

        if (!isset($dailyMap[$date])) {
            $dailyMap[$date] = 0.0;
        }
        if (!isset($categoryMap[$category])) {
            $categoryMap[$category] = 0.0;
        }
        if (!isset($productMap[$product])) {
            $productMap[$product] = ['sales' => 0.0, 'quantity' => 0];
        }
        $dailyMap[$date] += $lineTotal;
        $categoryMap[$category] += $lineTotal;
        $productMap[$product]['sales'] += $lineTotal;
        $productMap[$product]['quantity'] += $row['quantity'];
    }

    $dailySales = [];
    foreach ($dailyMap as $date => $sales) {
        $dailySales[] = ['date' => $date, 'sales' => normalize_number((float)$sales)];
    }
    usort($dailySales, fn (array $a, array $b): int => strcmp((string)$a['date'], (string)$b['date']));

    $categorySales = [];
    foreach ($categoryMap as $category => $sales) {
        $categorySales[] = ['category' => $category, 'sales' => normalize_number((float)$sales)];
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
            'product' => $product,
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

    $best = $dailySales[0];
    foreach ($dailySales as $item) {
        if ((float)$item['sales'] > (float)$best['sales']) {
            $best = $item;
        }
    }
    $totalRevenueRaw = 0.0;
    foreach ($dailySales as $item) {
        $totalRevenueRaw += (float)$item['sales'];
    }
    $totalRevenue = normalize_number($totalRevenueRaw);
    $summary = [
        // 入力CSVの注文明細数。
        'total_orders' => count($rows),
        // 全売上合計。
        'total_revenue' => $totalRevenue,
        // 注文1件あたり平均売上。
        'average_order_value' => normalize_number((float)$totalRevenue / count($rows)),
        // 売上最大日。
        'best_sales_day' => $best['date'],
        // 売上最大日の売上。
        'best_sales_amount' => $best['sales'],
    ];

    $artifacts = [
        // 日次売上グラフファイル名。
        'daily_sales_chart' => 'daily_sales.svg',
        // カテゴリ売上グラフファイル名。
        'category_sales_chart' => 'category_sales.svg',
        // 上位商品グラフファイル名。
        'top_products_chart' => 'top_products.svg',
        // 意思決定レポートファイル名。
        'decision_report_markdown' => 'decision_report.md',
    ];

    $insights = build_insights($summary, $categorySales, $topProducts);

    $dailyPoints = [];
    foreach ($dailySales as $item) {
        $dailyPoints[] = ['label' => $item['date'], 'value' => (float)$item['sales']];
    }
    $categoryPoints = [];
    foreach ($categorySales as $item) {
        $categoryPoints[] = ['label' => $item['category'], 'value' => (float)$item['sales']];
    }
    $productPoints = [];
    foreach ($topProducts as $item) {
        $productPoints[] = ['label' => $item['product'], 'value' => (float)$item['sales']];
    }

    file_put_contents($outputDir . '/' . $artifacts['daily_sales_chart'], build_line_svg('Daily Sales Trend', $dailyPoints));
    file_put_contents($outputDir . '/' . $artifacts['category_sales_chart'], build_bar_svg('Category Sales', $categoryPoints));
    file_put_contents($outputDir . '/' . $artifacts['top_products_chart'], build_bar_svg('Top Products by Sales', $productPoints));
    write_markdown_report($outputDir . '/' . $artifacts['decision_report_markdown'], $summary, $insights, $artifacts);

    return [
        // 売上全体の主要指標。
        'summary' => $summary,
        // 日単位の売上推移。
        'daily_sales' => $dailySales,
        // カテゴリ単位の売上。
        'category_sales' => $categorySales,
        // 売上上位商品の明細。
        'top_products' => $topProducts,
        // 意思決定のための短い示唆。
        'insights' => $insights,
        // 生成したグラフ/レポートのファイル名。
        'artifacts' => $artifacts,
    ];
}

$options = parse_args($argv);
$result = build_visual_sales_report($options['input'], $options['output_dir']);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
