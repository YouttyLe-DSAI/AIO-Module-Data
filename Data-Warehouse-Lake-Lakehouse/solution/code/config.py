"""Cấu hình dùng chung cho 4 script thực hành.

Toàn bộ dữ liệu sinh ra nằm trong thư mục LAB_DIR (mặc định: ./lakehouse_lab
cạnh file này). Xóa thư mục đó là reset toàn bộ bài lab.
"""

import os
import urllib.request
from pathlib import Path

LAB_DIR = Path(os.environ.get("LAB_DIR", Path(__file__).resolve().parent / "lakehouse_lab"))

# Bronze: Data Lake, chỉ là thư mục file theo quy ước Hive-style partition
BRONZE_DIR = LAB_DIR / "bronze" / "yellow_tripdata"

# Silver: Lakehouse, Iceberg warehouse + catalog SQLite
SILVER_WAREHOUSE = LAB_DIR / "silver_iceberg"
CATALOG_DB = SILVER_WAREHOUSE / "catalog.db"
SILVER_TABLE = "silver.yellow_trips"

# Gold: Data Warehouse, một file DuckDB
GOLD_DB = LAB_DIR / "gold_warehouse.duckdb"

NYC_TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
MONTHS = ["2023-01", "2023-02"]


def bronze_file(ym: str) -> Path:
    """Đường dẫn file Bronze của một tháng, ví dụ year=2023/month=01/part-0.parquet."""
    year, month = ym.split("-")
    return BRONZE_DIR / f"year={year}" / f"month={month}" / "part-0.parquet"


def download_month(ym: str) -> Path:
    """Tải file Parquet gốc của NYC TLC về vùng Bronze (bỏ qua nếu đã có)."""
    target = bronze_file(ym)
    if target.exists():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    url = NYC_TLC_URL.format(ym=ym)
    print(f"Đang tải {url} ...")
    tmp = target.with_suffix(".tmp")
    urllib.request.urlretrieve(url, tmp)
    tmp.rename(target)
    return target


def load_lab_catalog():
    """Mở Iceberg catalog cục bộ: SQLite giữ con trỏ metadata, file nằm trên đĩa."""
    from pyiceberg.catalog.sql import SqlCatalog

    SILVER_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "lab",
        uri=f"sqlite:///{CATALOG_DB}",
        warehouse=SILVER_WAREHOUSE.as_uri(),
    )
