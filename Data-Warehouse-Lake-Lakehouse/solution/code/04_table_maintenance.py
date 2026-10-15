"""Bước 4 (mở rộng) - Bảo trì bảng Lakehouse (Day-2 operations).

Mục tiêu:
  1. Tạo "small files problem": 300 micro-batch nhỏ -> 300 snapshot, 300 file dữ liệu.
  2. Đo chi phí lập kế hoạch truy vấn (query planning) và kích thước metadata.
  3. Compaction: gộp file nhỏ thành file lớn.
  4. Expire snapshots và dọn orphan files để thu hồi dung lượng.
"""

import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import GreaterThan

from config import SILVER_TABLE, SILVER_WAREHOUSE, load_lab_catalog

STREAM_TABLE = "silver.trips_stream"
STREAM_DIR = SILVER_WAREHOUSE / "silver" / "trips_stream"
N_BATCHES, BATCH_ROWS = 300, 1_000


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def dir_stats(path: Path) -> tuple[int, float]:
    files = [f for f in path.rglob("*") if f.is_file()]
    return len(files), sum(f.stat().st_size for f in files) / 1e6


def report(tbl, label: str) -> None:
    tbl = tbl.refresh()
    start = time.perf_counter()
    tasks = list(tbl.scan(row_filter=GreaterThan("fare_amount", 100.0)).plan_files())
    plan_ms = (time.perf_counter() - start) * 1000
    n_data, data_mb = dir_stats(STREAM_DIR / "data")
    n_meta, meta_mb = dir_stats(STREAM_DIR / "metadata")
    print(
        f"[{label}]\n"
        f"  snapshot trong metadata : {len(tbl.snapshots())}\n"
        f"  file dữ liệu đang dùng  : {len(tbl.inspect.files())}\n"
        f"  file cần đọc cho query  : {len(tasks)}  (lập kế hoạch mất {plan_ms:.0f} ms)\n"
        f"  thư mục data/           : {n_data} file, {data_mb:.1f} MB\n"
        f"  thư mục metadata/       : {n_meta} file, {meta_mb:.1f} MB"
    )


catalog = load_lab_catalog()
source = catalog.load_table(SILVER_TABLE).scan(limit=N_BATCHES * BATCH_ROWS).to_arrow()
try:
    catalog.drop_table(STREAM_TABLE)
except NoSuchTableError:
    pass
shutil.rmtree(STREAM_DIR, ignore_errors=True)
tbl = catalog.create_table(STREAM_TABLE, schema=source.schema)

# ---------------------------------------------------------------------------
section(f"1. Ghi {N_BATCHES} micro-batch, mỗi batch {BATCH_ROWS:,} dòng")
start = time.perf_counter()
for i in range(N_BATCHES):
    tbl.append(source.slice(i * BATCH_ROWS, BATCH_ROWS))
print(f"Ghi xong sau {time.perf_counter() - start:.1f} giây")
report(tbl, "Sau streaming")

# ---------------------------------------------------------------------------
section("2. Compaction: ghi lại toàn bộ dữ liệu thành ít file lớn")
# PyIceberg chưa có rewrite_data_files; với bảng nhỏ có thể overwrite toàn bộ.
# Ở môi trường production dùng Spark: CALL system.rewrite_data_files('db.table')
tbl.overwrite(tbl.scan().to_arrow())
report(tbl, "Sau compaction")

# ---------------------------------------------------------------------------
section("3. Expire snapshots: chỉ giữ snapshot hiện tại")
current_ts = datetime.fromtimestamp(tbl.current_snapshot().timestamp_ms / 1000, tz=timezone.utc)
tbl.maintenance.expire_snapshots().older_than(current_ts).commit()
report(tbl, "Sau expire snapshots")
print("Số snapshot đã giảm, nhưng file dữ liệu cũ vẫn nằm trên đĩa: chúng đã thành orphan files.")

# ---------------------------------------------------------------------------
section("4. Giới hạn metadata.json cũ và dọn orphan files")
# Mỗi commit sinh một metadata.json mới chứa toàn bộ lịch sử snapshot, nên metadata/ phình rất nhanh.
# Hai thuộc tính sau bảo Iceberg chỉ giữ 5 phiên bản metadata.json gần nhất.
with tbl.transaction() as tx:
    tx.set_properties({
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max": "5",
    })
tbl = tbl.refresh()

# File còn được tham chiếu: data file, manifest, manifest list của snapshot còn lại, metadata.json trong log
referenced = {urlparse(p).path for p in tbl.inspect.all_files()["file_path"].to_pylist()}
referenced |= {urlparse(p).path for p in tbl.inspect.all_manifests()["path"].to_pylist()}
referenced |= {urlparse(s.manifest_list).path for s in tbl.snapshots()}
referenced |= {urlparse(e.metadata_file).path for e in tbl.metadata.metadata_log}
referenced.add(urlparse(tbl.metadata_location).path)

orphans = [f for f in STREAM_DIR.rglob("*") if f.is_file() and str(f) not in referenced]
print(f"Tìm thấy {len(orphans)} file không còn được tham chiếu (data + manifest cũ).")
for f in orphans:
    f.unlink()
# Production: CALL system.remove_orphan_files(table => 'db.table', older_than => ...)
# và luôn đặt older_than đủ xa để không xóa nhầm file của job đang ghi dở.
report(tbl, "Sau dọn dẹp")
print(f"\nKiểm tra dữ liệu vẫn đủ: {tbl.scan().count():,} dòng")
