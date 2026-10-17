# Kịch bản video 60 giây: Data Warehouse, Data Lake và Data Lakehouse

- **Định dạng:** 16:9, 1920×1080, 30 fps, dài khoảng 60 giây
- **Người xem:** học viên Data Engineering đã biết SQL cơ bản
- **Thông điệp chính:** ba kiến trúc khác nhau ở chỗ *ai sở hữu "bảng"*, và hệ thống thực tế dùng cả ba theo Bronze → Silver → Gold
- **Lời thoại:** khoảng 170 tiếng, không cảnh nào vượt 3,3 tiếng/giây để người xem kịp đọc chữ trên màn hình

## Bảng phân cảnh

| # | Thời gian | Hình ảnh | Chữ trên màn hình | Lời thoại |
|---|---|---|---|---|
| 1 | 0:00–0:06 | **[AI-1]** Taxi vàng New York ban đêm, chậm rãi zoom vào | `3.000.000 chuyến / tháng` | Mỗi tháng, taxi vàng New York chạy ba triệu chuyến. Dữ liệu đó nên nằm ở đâu? |
| 2 | 0:06–0:16 | **[AI-2]** Nhà kho ngăn nắp, cửa có khóa | `DATA WAREHOUSE` · `SQL nhanh, ACID` · `Đắt, bị khóa vào vendor` | Data Warehouse như một nhà kho sắp sẵn: SQL rất nhanh, dữ liệu luôn sạch. Nhưng đắt, và bị khóa vào một vendor. |
| 3 | 0:16–0:22 | **[AI-3]** Hồ nước trong, nhiều file trôi trên mặt hồ | `DATA LAKE` · `Rẻ, chứa mọi loại file` | Data Lake đổ mọi file vào object storage, vừa rẻ vừa linh hoạt. |
| 4 | 0:22–0:30 | **[AI-4]** Hồ đó chuyển dần thành đầm lầy (chuyển cảnh crossfade từ AI-3) | `Tháng 2/2023: 7 cột đổi schema` · `Job chết giữa chừng → thấy 50% dữ liệu` | Nhưng không ai sở hữu bảng. Nguồn đổi schema, job ghi dở dang, và hồ dữ liệu biến thành đầm lầy. |
| 5 | 0:30–0:42 | **[SƠ ĐỒ]** Hình 6 trong bài đọc (cây metadata Iceberg), hiện lần lượt từ catalog xuống file Parquet | `DATA LAKEHOUSE` · `Snapshot · ACID · Time travel` | Data Lakehouse giữ file trên object storage và thêm lớp metadata như Apache Iceberg. Mỗi lần ghi là một snapshot nguyên tử, nhiều engine dùng chung. |
| 6 | 0:42–0:52 | **[SƠ ĐỒ]** Hình 7 trong bài đọc (Bronze → Silver → Gold), sáng lần lượt từng lớp | `Lake → Bronze` · `Lakehouse → Silver` · `Warehouse → Gold` | Trên thực tế, ta dùng cả ba: Data Lake cho Bronze, Lakehouse cho Silver, Warehouse cho Gold. |
| 7 | 0:52–1:00 | **[QUAY MÀN HÌNH]** Terminal chạy `02_silver_lakehouse.py`, dừng ở dòng `Truy vấn ngày 15/02 chỉ cần đọc 1/29 file` | Tên bài học + `github.com/YouttyLe-DSAI/AIO-Module-Data` | Bài đọc, quiz và code chạy thật đều có trong bộ tài liệu. Hãy tự chạy thử nhé! |

Nguồn hình:
- **[AI-n]:** tạo bằng Antigravity theo prompt bên dưới.
- **[SƠ ĐỒ]:** lấy từ hình trong `reading.pdf`, vì cần đúng chữ và đúng kỹ thuật.
- **[QUAY MÀN HÌNH]:** quay thật bằng OBS hoặc công cụ quay màn hình của hệ điều hành.

## Prompt tạo hình cho Antigravity

Thêm **câu khóa phong cách** vào cuối mọi prompt, để 4 hình cùng một phong cách:

> Flat vector illustration, slightly isometric, deep navy background, accent colors forest green #344F1E and indigo #333D91, soft rim lighting, clean shapes, cinematic composition, 16:9 widescreen, 1920x1080, no text, no letters, no logos.

| Mã | Prompt |
|---|---|
| AI-1 | A row of yellow New York City taxis on a wet avenue at night, neon reflections on the asphalt, glowing data points rising from the taxis into the sky like particles. |
| AI-2 | A huge, perfectly organized warehouse interior, identical shelves with neatly labeled boxes in straight rows, a single heavy steel door with a padlock in the foreground. |
| AI-3 | A calm, clear mountain lake seen from above, many colorful document and file icons floating on the water surface, sense of openness and low cost. |
| AI-4 | The same lake turned into a murky swamp, file icons half sunken, torn and tangled in reeds, broken pipes leaking, fog, sense of disorder. Keep the same camera angle as a clear lake scene. |

Ghi chú khi tạo hình:
1. **Giữ đúng 16:9:** hình ra vuông (1:1) thì yêu cầu lại ngay trong cùng phiên "regenerate at 16:9, 1920x1080". Nếu vẫn sai, mở rộng khung (outpaint) hoặc crop khi dựng.
2. **Không để AI viết chữ:** model tạo ảnh hay viết sai chữ. Mọi chữ đều thêm ở bước dựng video.
3. **AI-3 và AI-4 phải cùng góc máy:** tạo AI-3 trước, rồi đưa chính hình đó vào Antigravity kèm prompt AI-4 để sửa thành đầm lầy, như vậy crossfade mới liền.

## Quy trình thực hiện

1. **Claude:** viết kịch bản và prompt (file này). Nếu cần, Claude xuất Hình 6 và Hình 7 thành PNG 1920×1080 nền trong suốt, và tạo file phụ đề `.srt` khớp từng cảnh.
2. **Antigravity:** tạo 4 hình AI-1 đến AI-4 (mỗi hình 2–3 biến thể, chọn bản đẹp nhất), lưu vào `video/assets/`.
3. **Lời thoại:** tự đọc (tự nhiên nhất) hoặc dùng TTS tiếng Việt. Thu theo từng cảnh để dễ khớp thời gian.
4. **Quay màn hình:** chạy `python 02_silver_lakehouse.py`, quay khoảng 8 giây quanh dòng kết quả partition pruning.
5. **Dựng (CapCut hoặc DaVinci Resolve):** project 1920×1080, 30 fps. Hình tĩnh thêm chuyển động zoom/pan chậm. Chữ dùng màu xanh AIVN `#344F1E` hoặc trắng, giữ trong vùng an toàn cách mép 5%. Nhạc nền nhỏ hơn lời thoại khoảng 18–20 dB.
6. **Kiểm tra:** tổng thời lượng ≤ 60 giây, phụ đề đúng chính tả, số liệu khớp bài đọc (3 triệu chuyến/tháng, 7 cột đổi schema, 50% dữ liệu, 1/29 file).

## Lưu ý về quy định AIVN

Hướng dẫn Self-Learning cấm dùng hình do AI tạo trong tài liệu bài học. Video không nằm trong 5 sản phẩm bắt buộc, nhưng nếu định nộp kèm bài, nên hỏi BTC trước khi dùng hình AI-1 đến AI-4. Nếu không được dùng, thay bằng ảnh có giấy phép tự do (ghi rõ nguồn) hoặc tự vẽ.
