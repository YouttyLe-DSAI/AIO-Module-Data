# Data Warehouse, Data Lake và Data Lakehouse

Bộ tài liệu Self-Learning (AIO2026) cho bài học về ba kiến trúc dữ liệu phân tích, kèm phần thực hành Bronze → Silver → Gold với dữ liệu NYC Yellow Taxi tháng 1–2/2023.

## Cấu trúc thư mục

| Đường dẫn | Nội dung |
| --- | --- |
| `description/reading.tex`, `reading.pdf` | Bài đọc (29 trang), soạn trên template LaTeX của AIVN |
| `description/quiz.tex`, `quiz.pdf` | 14 câu trắc nghiệm |
| `description/references.bib` | Tài liệu tham khảo của bài đọc |
| `solution/solution.tex`, `solution.pdf` | Lời giải: đề, đáp án và giải thích cho từng câu |
| `solution/code/` | 4 script thực hành và `requirements.txt` |

Thư mục `slide/` chưa có. Theo quy định của chương trình, slide phải do người làm bài tự thiết kế trên template slide của AIVN.

## Biên dịch LaTeX

Mỗi thư mục `description/` và `solution/` đã có đủ file `.sty` và logo, có thể tải nguyên thư mục lên Overleaf.

- Compiler: **pdfLaTeX**.
- Bài đọc dùng biblatex với backend bibtex. Overleaf tự chạy bibtex. Nếu biên dịch trên máy, chạy theo thứ tự: `pdflatex reading` → `bibtex reading` → `pdflatex reading` → `pdflatex reading`.
- `quiz.tex` và `solution.tex` không có trích dẫn, chỉ cần chạy `pdflatex` hai lần.

## Chạy phần thực hành

Cần Python 3.10 trở lên và khoảng 300 MB dung lượng trống. Không cần Docker hay tài khoản cloud.

```bash
cd solution/code
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python 01_bronze_data_lake.py      # Bronze: Data Lake và các lỗi data swamp (~20 giây, tự tải dữ liệu)
python 02_silver_lakehouse.py      # Silver: bảng Iceberg, schema/partition evolution, time travel (~5 giây)
python 03_gold_warehouse.py        # Gold: star schema, kiểm tra chất lượng, data mart trên DuckDB (~5 giây)
python 04_table_maintenance.py     # Mở rộng: small files, compaction, expire snapshots, orphan files (~40 giây)
```

Các script phải chạy theo thứ tự trên. Toàn bộ dữ liệu sinh ra nằm trong `solution/code/lakehouse_lab/`; xóa thư mục này là reset bài lab. Muốn đổi vị trí, đặt biến môi trường `LAB_DIR`.

Nguồn dữ liệu: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
