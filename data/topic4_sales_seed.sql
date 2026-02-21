-- お題4用: SQL連携サンプルデータ
-- PostgreSQLへ投入し、3言語共通で同一出力比較に使う。

CREATE TABLE IF NOT EXISTS sales_orders (
  order_id TEXT PRIMARY KEY,
  order_date DATE NOT NULL,
  customer_id TEXT NOT NULL,
  customer_segment TEXT NOT NULL,
  payment_method TEXT NOT NULL,
  region TEXT NOT NULL,
  order_amount NUMERIC(10, 2) NOT NULL
);

TRUNCATE TABLE sales_orders;

INSERT INTO sales_orders (
  order_id,
  order_date,
  customer_id,
  customer_segment,
  payment_method,
  region,
  order_amount
) VALUES
  ('O1001', '2026-02-01', 'C001', 'Enterprise', 'Card', 'East', 120.00),
  ('O1002', '2026-02-01', 'C002', 'SMB', 'BankTransfer', 'West', 80.50),
  ('O1003', '2026-02-02', 'C003', 'Consumer', 'Card', 'East', 45.00),
  ('O1004', '2026-02-02', 'C004', 'Enterprise', 'Invoice', 'North', 560.00),
  ('O1005', '2026-02-03', 'C002', 'SMB', 'Card', 'West', 150.00),
  ('O1006', '2026-02-03', 'C005', 'Consumer', 'Cash', 'South', 35.50),
  ('O1007', '2026-02-04', 'C001', 'Enterprise', 'BankTransfer', 'East', 220.00),
  ('O1008', '2026-02-04', 'C006', 'SMB', 'Card', 'North', 95.00),
  ('O1009', '2026-02-05', 'C007', 'Consumer', 'Card', 'East', 75.25),
  ('O1010', '2026-02-05', 'C004', 'Enterprise', 'Invoice', 'North', 610.00),
  ('O1011', '2026-02-06', 'C008', 'SMB', 'Cash', 'West', 40.00),
  ('O1012', '2026-02-06', 'C009', 'Consumer', 'Card', 'South', 130.00);
