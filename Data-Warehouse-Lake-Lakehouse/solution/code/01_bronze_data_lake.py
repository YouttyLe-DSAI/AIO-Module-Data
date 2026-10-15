"""Bước 1 - Bronze / Data Lake.

Mục tiêu:
  1. Đổ file Parquet gốc vào một thư mục theo quy ước Hive-style partition.
  2. Xem footer của file Parquet: row group, column chunk, thống kê min/max.
  3. Đọc "bảng" Data Lake bằng cách quét thư mục (schema-on-read).
  4. Tái hiện các lỗi kinh điển khi không có ai sở hữu bảng: schema drift,
     ghi dở dang (partial write), xóa vài dòng phải ghi lại cả file.
"""

import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from config import BRONZE_DIR, MONTHS, bronze_file, download_month

GLOB = f"{BRONZE_DIR}/**/*.parquet"


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


# ---------------------------------------------------------------------------
section("1. Landing: tải dữ liệu gốc vào Bronze")
for ym in MONTHS:
    path = download_month(ym)
    print(f"{ym}: {path.relative_to(BRONZE_DIR.parent)} ({path.stat().st_size / 1e6:.1f} MB)")

# ---------------------------------------------------------------------------
section("2. Bên trong một file Parquet (columnar storage)")
pf = pq.ParquetFile(bronze_file("2023-01"))
meta = pf.metadata
print(f"Số dòng: {meta.num_rows:,} | Số cột: {meta.num_columns} | Số row group: {meta.num_row_groups}")
rg = meta.row_group(0)
for name in ["tpep_pickup_datetime", "fare_amount", "trip_distance"]:
    idx = pf.schema_arrow.get_field_index(name)
    col = rg.column(idx)
    st = col.statistics
    print(
        f"  {name:<22} nén={col.compression:<6} "
        f"{col.total_compressed_size / 1e6:6.1f} MB  min={st.min}  max={st.max}  null={st.null_count}"
    )
print("=> Engine đọc footer trước, chỉ đọc đúng các cột cần và bỏ qua row group không thỏa điều kiện.")

# ---------------------------------------------------------------------------
section("3. Đọc Data Lake: 'bảng' chỉ là một glob thư mục")
con = duckdb.connect()
con.sql(
    f"""
    SELECT year, month, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
    FROM read_parquet('{GLOB}', hive_partitioning = true)
    GROUP BY ALL ORDER BY ALL
    """
).show()

# ---------------------------------------------------------------------------
section("4a. Lỗi 1 - Schema drift có thật: nguồn đổi schema giữa hai tháng")
jan_schema = pq.read_schema(bronze_file("2023-01"))
feb_schema = pq.read_schema(bronze_file("2023-02"))
for f_jan, f_feb in zip(jan_schema, feb_schema):
    if f_jan.name != f_feb.name or f_jan.type != f_feb.type:
        print(f"  {f_jan.name:<20} {str(f_jan.type):<8} ->  {f_feb.name:<20} {f_feb.type}")
print("Engine đọc glob phải tự đoán cách hợp nhất; mỗi engine có thể đoán khác nhau.")

# ---------------------------------------------------------------------------
section("4b. Lỗi 2 - Một job khác ghi file sai kiểu vào cùng prefix")
rogue_path = bronze_file("2023-01").parent / "part-1-rogue.parquet"
rogue = pa.table(
    {
        "VendorID": ["INVALID_ID"],          # đáng lẽ là int64
        "tpep_pickup_datetime": ["not-a-timestamp"],
        "fare_amount": [-9999.99],
    }
)
pq.write_table(rogue, rogue_path)
try:
    con.sql(f"SELECT VendorID, avg(fare_amount) FROM read_parquet('{GLOB}') GROUP BY 1").show()
except duckdb.Error as exc:
    reason = str(exc).split("failed to cast", 1)[-1].strip(" :")
    print(f"Truy vấn thất bại ({type(exc).__name__}): failed to cast {reason}")
rogue_path.unlink()
print("Không có lớp nào kiểm tra schema khi ghi, nên lỗi chỉ lộ ra khi người khác đọc.")

# ---------------------------------------------------------------------------
section("4c. Lỗi 3 - Partial write: job ghi 4 file nhưng chết sau file thứ 2")
feb = pq.read_table(bronze_file("2023-02"))
expected_rows = feb.num_rows
staging_dir = bronze_file("2023-02").parent
bronze_file("2023-02").rename(staging_dir / "backup.parquet.bak")  # tạm cất bản gốc
chunk = expected_rows // 4 + 1
for i in range(4):
    if i == 2:
        print("  !!! job bị kill khi đang ghi part-2")
        break
    pq.write_table(feb.slice(i * chunk, chunk), staging_dir / f"part-{i}.parquet")
    print(f"  đã ghi part-{i}.parquet")

seen = con.sql(
    f"SELECT count(*) FROM read_parquet('{BRONZE_DIR}/year=2023/month=02/*.parquet')"
).fetchone()[0]
print(f"Reader thấy {seen:,} dòng / {expected_rows:,} dòng ({seen / expected_rows:.0%}), không có cảnh báo nào.")
for f in staging_dir.glob("part-*.parquet"):
    f.unlink()
(staging_dir / "backup.parquet.bak").rename(bronze_file("2023-02"))

# ---------------------------------------------------------------------------
section("4d. Lỗi 4 - Không có DELETE: xóa vài dòng phải ghi lại cả file")
jan_path = bronze_file("2023-01")
jan = pq.read_table(jan_path)
bad = pc.greater(jan["fare_amount"], 500)
print(f"Cần xóa {pc.sum(bad).as_py()} chuyến có fare_amount > 500 trong {jan.num_rows:,} dòng.")
start = time.perf_counter()
tmp_path = jan_path.with_suffix(".rewrite")
pq.write_table(jan.filter(pc.invert(bad)), tmp_path)
elapsed = time.perf_counter() - start
tmp_path.unlink()  # giữ nguyên Bronze bất biến, chỉ đo chi phí
print(f"Phải đọc và ghi lại toàn bộ file ({jan_path.stat().st_size / 1e6:.0f} MB) mất {elapsed:.1f} giây.")
print("Trong lúc đổi file, reader khác có thể thấy file cũ, file mới, hoặc không thấy file nào.")

print("\nBronze được giữ nguyên làm kho lưu trữ gốc. Chạy tiếp: python 02_silver_lakehouse.py")
