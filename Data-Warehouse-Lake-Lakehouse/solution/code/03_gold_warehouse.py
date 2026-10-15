"""Bước 3 - Gold / Data Warehouse với DuckDB.

Mục tiêu:
  1. DuckDB đọc trực tiếp snapshot hiện tại của bảng Iceberg Silver.
  2. Xây Star Schema theo Kimball: dim_vendor, dim_payment_type, dim_date, fact_trips.
  3. Kiểm tra chất lượng dữ liệu: đối soát số dòng, khóa ngoại "mồ côi".
  4. Tạo data mart tổng hợp cho dashboard.
"""

from urllib.parse import urlparse

import duckdb

from config import GOLD_DB, SILVER_TABLE, load_lab_catalog


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


silver = load_lab_catalog().load_table(SILVER_TABLE)
metadata_path = urlparse(silver.metadata_location).path  # bỏ tiền tố file://
silver_rows = silver.scan().count()

con = duckdb.connect(str(GOLD_DB))
con.execute("INSTALL iceberg; LOAD iceberg;")

# ---------------------------------------------------------------------------
section("1. Đọc bảng Silver qua metadata Iceberg (không quét thư mục)")
print(f"metadata: ...{metadata_path[-60:]}")
con.sql(
    f"""
    SELECT count(*) AS rows, min(pickup_at) AS first_pickup, max(pickup_at) AS last_pickup
    FROM iceberg_scan('{metadata_path}')
    """
).show()

# ---------------------------------------------------------------------------
section("2. Dimension tables")
con.execute(
    """
    -- Mã nhà cung cấp theo NYC TLC Data Dictionary (bản 2025)
    CREATE OR REPLACE TABLE dim_vendor AS
    SELECT * FROM (VALUES
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'Curb Mobility, LLC'),
        (6, 'Myle Technologies Inc'),
        (7, 'Helix')
    ) AS t(vendor_key, vendor_name);

    CREATE OR REPLACE TABLE dim_payment_type AS
    SELECT * FROM (VALUES
        (0, 'Flex Fare trip', TRUE),
        (1, 'Credit card',    TRUE),
        (2, 'Cash',           TRUE),
        (3, 'No charge',      FALSE),
        (4, 'Dispute',        FALSE),
        (5, 'Unknown',        FALSE),
        (6, 'Voided trip',    FALSE)
    ) AS t(payment_type_key, payment_name, is_revenue);

    -- dim_date sinh từ lịch, khóa dạng YYYYMMDD
    CREATE OR REPLACE TABLE dim_date AS
    SELECT
        CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
        CAST(d AS DATE)                        AS full_date,
        dayname(d)                             AS day_name,
        isodow(d) >= 6                         AS is_weekend,
        month(d)                               AS month,
        year(d)                                AS year
    FROM range(DATE '2023-01-01', DATE '2023-03-01', INTERVAL 1 DAY) AS t(d);
    """
)
for t in ["dim_vendor", "dim_payment_type", "dim_date"]:
    print(f"{t:<18}: {con.sql(f'SELECT count(*) FROM {t}').fetchone()[0]} dòng")

# ---------------------------------------------------------------------------
section("3. Fact table: grain = 1 dòng / 1 chuyến xe")
con.execute(
    f"""
    CREATE OR REPLACE TABLE fact_trips AS
    SELECT
        row_number() OVER (ORDER BY pickup_at)            AS trip_key,     -- surrogate key
        vendor_id                                         AS vendor_key,
        payment_type                                      AS payment_type_key,
        CAST(strftime(pickup_at, '%Y%m%d') AS INTEGER)    AS pickup_date_key,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        date_diff('minute', pickup_at, dropoff_at)        AS duration_min,
        fare_amount,
        tip_amount,
        total_amount,
        coalesce(tip_per_mile, tip_amount / trip_distance) AS tip_per_mile
    FROM iceberg_scan('{metadata_path}')
    """
)
fact_rows = con.sql("SELECT count(*) FROM fact_trips").fetchone()[0]
print(f"fact_trips: {fact_rows:,} dòng")

# ---------------------------------------------------------------------------
section("4. Kiểm tra chất lượng dữ liệu (data tests)")
checks = {
    "Số dòng fact = số dòng Silver": f"SELECT {fact_rows} = {silver_rows}",
    "Không có vendor_key mồ côi": """
        SELECT count(*) = 0 FROM fact_trips f
        LEFT JOIN dim_vendor v USING (vendor_key) WHERE v.vendor_key IS NULL""",
    "Không có payment_type_key mồ côi": """
        SELECT count(*) = 0 FROM fact_trips f
        LEFT JOIN dim_payment_type p USING (payment_type_key) WHERE p.payment_type_key IS NULL""",
    "Không có pickup_date_key mồ côi": """
        SELECT count(*) = 0 FROM fact_trips f
        LEFT JOIN dim_date d ON f.pickup_date_key = d.date_key WHERE d.date_key IS NULL""",
    "trip_key là duy nhất": "SELECT count(DISTINCT trip_key) = count(*) FROM fact_trips",
}
for name, sql in checks.items():
    ok = con.sql(sql).fetchone()[0]
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")

lost = con.sql(
    """
    SELECT count(*) FROM fact_trips f
    LEFT JOIN (VALUES (1), (2), (3), (4), (5)) AS old_dim(k) ON f.payment_type_key = old_dim.k
    WHERE old_dim.k IS NULL
    """
).fetchone()[0]
print(f"\nNếu dim_payment_type thiếu mã 0, INNER JOIN sẽ âm thầm làm mất {lost:,} chuyến.")

# ---------------------------------------------------------------------------
section("5. Data mart: doanh thu theo ngày và nhà cung cấp")
con.execute(
    """
    CREATE OR REPLACE TABLE mart_daily_vendor_revenue AS
    SELECT
        d.full_date,
        d.is_weekend,
        v.vendor_name,
        count(*)                        AS trips,
        round(sum(f.total_amount), 2)   AS gross_revenue,
        round(avg(f.duration_min), 1)   AS avg_duration_min,
        round(avg(f.tip_per_mile), 2)   AS avg_tip_per_mile
    FROM fact_trips f
    JOIN dim_date d         ON f.pickup_date_key = d.date_key
    JOIN dim_vendor v       USING (vendor_key)
    JOIN dim_payment_type p USING (payment_type_key)
    WHERE p.is_revenue
    GROUP BY ALL
    ORDER BY d.full_date, v.vendor_name
    """
)
con.sql(
    """
    SELECT full_date, left(vendor_name, 20) AS vendor, trips, gross_revenue, avg_duration_min
    FROM mart_daily_vendor_revenue LIMIT 4
    """
).show()

print("Câu hỏi BI: cuối tuần khác ngày thường thế nào?")
con.sql(
    """
    SELECT is_weekend,
           round(avg(trips))         AS avg_trips_per_day_vendor,
           round(avg(gross_revenue)) AS avg_revenue_per_day_vendor
    FROM mart_daily_vendor_revenue
    GROUP BY is_weekend ORDER BY is_weekend
    """
).show()

con.close()
print(f"Gold warehouse đã lưu tại: {GOLD_DB}")
