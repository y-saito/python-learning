<?php

// このファイルは「お題2: JSONログ整形と異常値検出」の PHP 実装。
// 目的は、Python/Node.js と同じ入力・同じ出力で比較学習できること。
// PHP では連想配列と foreach を中心に、各行程を手続き的に明示する。

/**
 * CLI引数から入力JSONLパスを取得する。
 *
 * @param array<int, string> $argv CLI引数配列。
 *
 * @return string 入力JSONLパス。
 */
function parse_input_path(array $argv): string
{
    $count = count($argv);
    for ($i = 1; $i < $count; $i++) {
        if ($argv[$i] === '--input' && isset($argv[$i + 1])) {
            return $argv[$i + 1];
        }
    }

    throw new InvalidArgumentException('必須引数がありません: --input <jsonl_path>');
}

/**
 * 数値を比較しやすいJSON表現へ正規化する。
 *
 * @param float $value 正規化前の数値。
 *
 * @return float|int 正規化後の数値。
 */
function normalize_number(float $value): float|int
{
    // Python の round + is_integer、Node.js の toFixed + Number と同じ目的。
    $rounded = round($value, 2);
    if (fmod($rounded, 1.0) === 0.0) {
        return (int)$rounded;
    }

    return $rounded;
}

/**
 * 線形補間で分位点を計算する（pandas既定に寄せる）。
 *
 * @param array<int, float> $sortedValues 昇順ソート済み配列。
 * @param float $q 分位（0.0-1.0）。
 *
 * @return float 分位点。
 */
function quantile_linear(array $sortedValues, float $q): float
{
    $n = count($sortedValues);
    if ($n === 0) {
        throw new RuntimeException('分位点計算対象が空です');
    }
    if ($n === 1) {
        return $sortedValues[0];
    }

    $position = ($n - 1) * $q;
    $lowerIndex = (int)floor($position);
    $upperIndex = (int)ceil($position);
    $lowerValue = $sortedValues[$lowerIndex];
    $upperValue = $sortedValues[$upperIndex];

    if ($lowerIndex === $upperIndex) {
        return $lowerValue;
    }

    $weight = $position - $lowerIndex;
    return $lowerValue + ($upperValue - $lowerValue) * $weight;
}

/**
 * JSONログを整形し、異常値/外れ値を判定する。
 *
 * 実行順は以下で固定する。
 * 1) 入力読み込み（JSONL）
 * 2) 型変換（timestamp / status / response_time_ms）
 * 3) 欠損補完（補完ルールと件数カウント）
 * 4) 異常値チェック（業務ルール）
 * 5) 外れ値検出（IQR / 境界式）
 * 6) 出力整形（並び順・数値正規化・anomalies/outliers抽出）
 *
 * @param string $inputPath 入力JSONLパス。
 *
 * @return array<string, mixed> 結果JSON。
 */
function preprocess_logs(string $inputPath): array
{
    // --- 行程1: 入力読み込み（JSONL）---
    // PHP は file() で行配列を作り、各行を json_decode で連想配列化する。
    // Python は json_normalize 前提、Node.js は readFile + split + JSON.parse が相当。
    $lines = file($inputPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    if ($lines === false) {
        throw new RuntimeException("入力ファイルを開けません: {$inputPath}");
    }

    $parsed = [];

    foreach ($lines as $line) {
        $decoded = json_decode($line, true);
        if (!is_array($decoded)) {
            continue;
        }

        // --- 行程2: 型変換（timestamp / status / response_time_ms）---
        // timestamp は DateTimeImmutable へ変換し、失敗時は除外する。
        $timestampRaw = $decoded['timestamp'] ?? '';
        try {
            $timestamp = new DateTimeImmutable((string)$timestampRaw);
        } catch (Exception $e) {
            continue;
        }

        // status/response_time_ms は数値化可能な値のみ採用し、失敗は null で保持する。
        $statusRaw = $decoded['status'] ?? null;
        $responseRaw = $decoded['response_time_ms'] ?? null;

        $status = is_numeric($statusRaw) ? (float)$statusRaw : null;
        $responseTime = is_numeric($responseRaw) ? (float)$responseRaw : null;

        $endpointRaw = $decoded['endpoint'] ?? null;
        $methodRaw = $decoded['method'] ?? null;

        $endpoint = (is_string($endpointRaw) && trim($endpointRaw) !== '') ? $endpointRaw : null;
        $method = (is_string($methodRaw) && trim($methodRaw) !== '') ? $methodRaw : null;

        $parsed[] = [
            'timestamp' => $timestamp,
            'endpoint' => $endpoint,
            'method' => $method,
            'status' => $status,
            'response_time_ms' => $responseTime,
        ];
    }

    // --- 行程3: 欠損補完（補完ルールと件数カウント）---
    // 先に中央値計算用の response_time_ms 有効値を抽出する。
    $responseCandidates = [];
    foreach ($parsed as $row) {
        if ($row['response_time_ms'] !== null) {
            $responseCandidates[] = $row['response_time_ms'];
        }
    }
    sort($responseCandidates);

    $count = count($responseCandidates);
    if ($count === 0) {
        throw new RuntimeException('response_time_ms の有効値がありません');
    }
    $mid = (int)floor($count / 2);
    $medianResponseTime = ($count % 2 === 0)
        ? ($responseCandidates[$mid - 1] + $responseCandidates[$mid]) / 2
        : $responseCandidates[$mid];

    // 3言語共通ルール:
    // response_time_ms=中央値, status=0, endpoint=/unknown, method=UNKNOWN
    $filledResponseTimeCount = 0;
    $filledStatusCount = 0;
    $filledEndpointCount = 0;
    $filledMethodCount = 0;

    foreach ($parsed as &$row) {
        if ($row['response_time_ms'] === null) {
            $row['response_time_ms'] = $medianResponseTime;
            $filledResponseTimeCount += 1;
        }
        if ($row['status'] === null) {
            $row['status'] = 0.0;
            $filledStatusCount += 1;
        }
        if ($row['endpoint'] === null) {
            $row['endpoint'] = '/unknown';
            $filledEndpointCount += 1;
        }
        if ($row['method'] === null) {
            $row['method'] = 'UNKNOWN';
            $filledMethodCount += 1;
        }
    }
    unset($row);

    // --- 行程4: 異常値チェック（業務ルール）---
    // 異常値は「論理的にあり得ない値」を扱う。
    // - response_time_ms < 0
    // - status が 0（欠損補完用の許容値）でも 100-599 でもない
    foreach ($parsed as &$row) {
        $statusValue = (float)$row['status'];
        $responseValue = (float)$row['response_time_ms'];
        $row['is_anomaly'] = $responseValue < 0.0 || ($statusValue !== 0.0 && ($statusValue < 100.0 || $statusValue > 599.0));
    }
    unset($row);

    // --- 行程5: 外れ値検出（IQR / 境界式）---
    // Node.js/PHP は分位点計算を自前関数で行い、Python は quantile() で計算する。
    $responseTimes = [];
    foreach ($parsed as $row) {
        $responseTimes[] = (float)$row['response_time_ms'];
    }
    sort($responseTimes);

    $q1 = quantile_linear($responseTimes, 0.25);
    $q3 = quantile_linear($responseTimes, 0.75);
    $iqr = $q3 - $q1;
    $lowerBound = $q1 - 1.5 * $iqr;
    $upperBound = $q3 + 1.5 * $iqr;

    // --- 行程6: 出力整形（並び順・数値正規化・anomalies/outliers抽出）---
    // timestamp 昇順に固定して、比較時の順序差分を防ぐ。
    usort($parsed, function (array $a, array $b): int {
        /** @var DateTimeImmutable $left */
        $left = $a['timestamp'];
        /** @var DateTimeImmutable $right */
        $right = $b['timestamp'];
        return $left <=> $right;
    });

    $cleanedLogs = [];
    foreach ($parsed as $row) {
        $responseTime = (float)$row['response_time_ms'];
        $isOutlier = $responseTime < $lowerBound || $responseTime > $upperBound;

        /** @var DateTimeImmutable $timestamp */
        $timestamp = $row['timestamp'];

        $cleanedLogs[] = [
            'timestamp' => $timestamp->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d\TH:i:s\Z'),
            'endpoint' => (string)$row['endpoint'],
            'method' => (string)$row['method'],
            'status' => (int)$row['status'],
            'response_time_ms' => normalize_number($responseTime),
            'is_anomaly' => (bool)$row['is_anomaly'],
            'is_outlier' => $isOutlier,
        ];
    }

    $anomalies = array_values(array_filter($cleanedLogs, function (array $row): bool {
        return $row['is_anomaly'] === true;
    }));
    $outliers = array_values(array_filter($cleanedLogs, function (array $row): bool {
        return $row['is_outlier'] === true;
    }));

    // 返却スキーマ:
    // summary: 件数系の集計情報
    // response_time_bounds: IQR外れ値判定に使った下限/上限
    // cleaned_logs: 全件の整形済みログ
    // anomalies: 業務ルール異常値のみ
    // outliers: IQR外れ値のみ
    return [
        'summary' => [
            'total_records' => count($cleanedLogs),
            'filled_response_time_count' => $filledResponseTimeCount,
            'filled_status_count' => $filledStatusCount,
            'filled_endpoint_count' => $filledEndpointCount,
            'filled_method_count' => $filledMethodCount,
            'anomaly_count' => count($anomalies),
            'outlier_count' => count($outliers),
        ],
        'response_time_bounds' => [
            'lower' => normalize_number($lowerBound),
            'upper' => normalize_number($upperBound),
        ],
        'cleaned_logs' => $cleanedLogs,
        'anomalies' => $anomalies,
        'outliers' => $outliers,
    ];
}

// CLIエントリーポイント: 引数解釈 -> 前処理実行 -> 整形JSON出力。
$inputPath = parse_input_path($argv);
$result = preprocess_logs($inputPath);
echo json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . PHP_EOL;
