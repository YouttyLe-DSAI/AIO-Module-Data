"""Bước 2 - Silver / Data Lakehouse với Apache Iceberg (PyIceberg).

Mục tiêu:
  1. Chuẩn hóa (conform) dữ liệu Bronze về một schema đích và lọc dữ liệu lỗi.
  2. Tạo bảng Iceberg: catalog trở thành "chủ sở hữu" của bảng.
  3. Schema evolution và partition evolution chỉ thay đổi metadata.
  4. Schema enforcement: bảng từ chối dữ liệu sai kiểu.
  5. Row-level DELETE, xem lịch sử snapshot và time travel.
"""

import json
import shutil
from datetime import datetime
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, GreaterThan, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import DayTransform
from pyiceberg.types import DoubleType

from config import MONTHS, SILVER_TABLE, SILVER_WAREHOUSE, bronze_file, load_lab_catalog


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


# Schema đích của Silver: tên cột chữ thường, kiểu dữ liệu thống nhất giữa các tháng
SILVER_SCHEMA = pa.schema(
    [
        ("vendor_id", pa.int32()),
        ("pickup_at", pa.timestamp("us")),
        ("dropoff_at", pa.timestamp("us")),
        ("passenger_count", pa.int32()),
        ("trip_distance", pa.float64()),
        ("ratecode_id", pa.int32()),
        ("pu_location_id", pa.int32()),
        ("do_location_id", pa.int32()),
        ("payment_type", pa.int32()),
        ("fare_amount", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64()),
    ]
)

# Tên cột Bronze (viết thường) -> tên cột Silver
RENAME = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",
    "ratecodeid": "ratecode_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
}


def conform(ym: str) -> pa.Table:
    """Đọc một tháng Bronze, đổi tên + ép kiểu theo SILVER_SCHEMA, lọc dòng lỗi."""
    raw = pq.read_table(bronze_file(ym))
    # Tháng 01 có cột airport_fee, tháng 02 lại là Airport_fee: so khớp không phân biệt hoa thường
    raw = raw.rename_columns([RENAME.get(c.lower(), c.lower())
                              for c in raw.column_names])
    tbl = raw.select(SILVER_SCHEMA.names).cast(SILVER_SCHEMA)

    year, month = map(int, ym.split("-"))
    next_month = datetime(year + month // 12, month % 12 + 1, 1)
    start = pa.scalar(datetime(year, month, 1), pa.timestamp("us"))
    end = pa.scalar(next_month, pa.timestamp("us"))
    rules = {
        "pickup ngoài tháng": pc.or_(pc.less(tbl["pickup_at"], start),
                                     pc.greater_equal(tbl["pickup_at"], end)),
        "trip_distance <= 0": pc.less_equal(tbl["trip_distance"], 0),
        "fare_amount <= 0": pc.less_equal(tbl["fare_amount"], 0),
        "dropoff trước pickup": pc.less(tbl["dropoff_at"], tbl["pickup_at"]),
    }
    bad = pc.or_(pc.or_(rules["pickup ngoài tháng"], rules["trip_distance <= 0"]),
                 pc.or_(rules["fare_amount <= 0"], rules["dropoff trước pickup"]))
    clean = tbl.filter(pc.invert(bad))
    print(f"{ym}: Bronze {raw.num_rows:,} dòng -> Silver {clean.num_rows:,} dòng")
    for name, mask in rules.items():
        print(f"    loại vì {name:<22}: {pc.sum(mask).as_py():>7,}")
    return clean


def with_tip_per_mile(tbl: pa.Table) -> pa.Table:
    return tbl.append_column("tip_per_mile", pc.divide(tbl["tip_amount"], tbl["trip_distance"]))


TABLE_DIR = SILVER_WAREHOUSE / "silver" / "yellow_trips"


def list_metadata_files() -> None:
    files = sorted((TABLE_DIR / "metadata").iterdir(), key=lambda p: p.stat().st_mtime)
    for f in files:
        print(f"    {f.name:<75} {f.stat().st_size / 1024:7.1f} KB")


catalog = load_lab_catalog()
catalog.create_namespace_if_not_exists("silver")
# Chạy lại từ đầu: drop_table chỉ xóa con trỏ trong catalog, nên xóa luôn thư mục bảng
try:
    catalog.drop_table(SILVER_TABLE)
except NoSuchTableError:
    pass
shutil.rmtree(TABLE_DIR, ignore_errors=True)

# ---------------------------------------------------------------------------
section("1. Conform tháng 01 và tạo bảng Iceberg (snapshot 1)")
jan = conform("2023-01")
tbl = catalog.create_table(SILVER_TABLE, schema=SILVER_SCHEMA)
tbl.append(jan)
snap_jan = tbl.current_snapshot().snapshot_id
print(f"\nSnapshot 1 = {snap_jan}")
print(tbl.schema())
print("\nCác file metadata sau commit đầu tiên:")
list_metadata_files()

# ---------------------------------------------------------------------------
section("2. Schema evolution + partition evolution (chỉ sửa metadata)")
with tbl.update_schema() as update:
    update.add_column("tip_per_mile", DoubleType(), doc="tip_amount / trip_distance")
with tbl.update_spec() as spec:
    spec.add_field("pickup_at", DayTransform(), "pickup_day")
print(f"Schema hiện tại có {len(tbl.schema().fields)} cột, cột mới có field_id = "
      f"{tbl.schema().find_field('tip_per_mile').field_id}")
print(f"Partition spec hiện tại: {tbl.spec()}")
print(f"Snapshot hiện tại vẫn là {tbl.current_snapshot().snapshot_id}: chưa file dữ liệu nào bị ghi lại.")

# ---------------------------------------------------------------------------
section("3. Schema enforcement: thử ghi một batch sai kiểu")
rogue = pa.table({"vendor_id": ["INVALID_ID"], "pickup_at": ["not-a-timestamp"]})
try:
    tbl.append(rogue)
except ValueError as exc:
    print(f"Bị từ chối: {str(exc).splitlines()[0]}")
print(f"Snapshot hiện tại vẫn là {tbl.current_snapshot().snapshot_id}: lần ghi lỗi không để lại dấu vết.")

# ---------------------------------------------------------------------------
section("4. Append tháng 02 có cột mới, ghi theo partition spec mới (snapshot 2)")
feb = with_tip_per_mile(conform("2023-02"))
tbl.append(feb)
snap_feb = tbl.current_snapshot().snapshot_id
files = tbl.inspect.files().to_pandas()
print(files.groupby("spec_id").agg(so_file=("file_path", "count"), so_dong=("record_count", "sum")))
print("spec_id 0 = file tháng 01 (không partition), spec_id 1 = file tháng 02 (partition theo ngày).")

feb_15 = And(GreaterThanOrEqual("pickup_at", "2023-02-15T00:00:00"), LessThan("pickup_at", "2023-02-16T00:00:00"))
planned = list(tbl.scan(row_filter=feb_15).plan_files())
print(f"Truy vấn ngày 15/02 chỉ cần đọc {len(planned)}/{len(files)} file nhờ partition pruning.")

# ---------------------------------------------------------------------------
section("5. Row-level DELETE: xóa các chuyến fare_amount > 500 (snapshot 3)")
before = tbl.scan().count()
tbl.delete(delete_filter=GreaterThan("fare_amount", 500.0))
after = tbl.scan().count()
print(f"Số dòng: {before:,} -> {after:,} (xóa {before - after} dòng)")

# ---------------------------------------------------------------------------
section("6. Lịch sử snapshot và time travel")
for snap in tbl.snapshots():
    committed_at = datetime.fromtimestamp(snap.timestamp_ms / 1000).strftime("%H:%M:%S")
    print(f"  {committed_at}  snapshot={snap.snapshot_id:<20} "
          f"parent={str(snap.parent_snapshot_id):<20} {snap.summary.operation.value}")
tbl.manage_snapshots().create_tag(snap_jan, "after_jan_load").commit()

as_of_jan = tbl.scan(snapshot_id=snap_jan).count()
as_of_feb = tbl.scan(snapshot_id=snap_feb).count()
print(f"\nSnapshot 1 (tag after_jan_load): {as_of_jan:,} dòng")
print(f"Snapshot 2 (trước khi DELETE)  : {as_of_feb:,} dòng")
print(f"Hiện tại                       : {after:,} dòng")

jan_tip = tbl.scan(row_filter=LessThan("pickup_at", "2023-02-01T00:00:00"),
                   selected_fields=("tip_per_mile",), limit=3).to_arrow()
print(f"tip_per_mile của các dòng tháng 01 (ghi trước khi có cột): {jan_tip['tip_per_mile'].to_pylist()}")

print("\nThư mục metadata sau toàn bộ thao tác:")
list_metadata_files()
print(f"\nmetadata_location hiện tại: {tbl.metadata_location}")
current = json.loads(open(urlparse(tbl.metadata_location).path).read())
for key in ["format-version", "current-schema-id", "default-spec-id", "current-snapshot-id"]:
    print(f"    {key:<20}: {current[key]}")
print(f"    {'snapshots':<20}: {len(current['snapshots'])} snapshot")
print(f"    {'refs':<20}: {list(current['refs'])}")
print("Chạy tiếp: python 03_gold_warehouse.py")
