# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trương Thị Lan Anh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `<điền>` |
| GitHub | SxAinsworth |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `f7bbaac3feee`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `756 / 123 / 140` (tổng 1019 khung; 18 / 4 / 4 trip)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Ở bronze, A có 3021 dòng, B có 3234 dòng. Hai dòng đầu mỗi bảng cho thấy B khác A ở:
1. **Thời gian**: A là chuỗi ISO UTC (`ts_utc` = `2025-10-07T08:00:00.014Z`), B là epoch mili giây (`timestamp_ms` = `1759933801509`).
2. **Đơn vị tốc độ**: A là m/s (`speed_mps` = 1.213), B là km/h (`spd_kmh` = 55.11).
3. **Tên cột**: `heading_deg/lat/lon/alt_m/acc_x/acc_y/steer` ở A thành `hdg/latitude/longitude/altitude/ax/ay/steering` ở B.
4. **Bàn đạp**: A là tỉ lệ 0–1 (`gas` = 0.028, `brake`), B là phần trăm (`throttle_pct` = 2.0, `brake_pct`).
5. **Đồng hồ**: `bronze_video_files` ghi `telemetry_clock_offset_ms` = 0 cho drive_a nhưng 1500 cho drive_b, tức đồng hồ thiết bị B lệch 1,5 s so với video.

Bronze là bản ghi trung thực của nguồn: gộp hai nguồn ngay ở đây đã là một phép biến đổi (đổi tên, đổi đơn vị), nếu logic đó sai thì không còn bản gốc để chạy lại. Giữ mọi cột là chữ (14 cột dữ liệu của B đều `VARCHAR`) để lần nạp không bao giờ hỏng vì dữ liệu bẩn: mỗi nguồn có 30 ô tốc độ trống, ép kiểu ngay thì hoặc lỗi cả lô, hoặc bị đổi im lặng thành NULL/0 và mất dấu lỗi gốc. Việc ép kiểu, đổi đơn vị, trừ độ lệch đồng hồ và cách ly dòng lỗi để sang Silver. Mỗi dòng còn mang `_source_file`, `_batch_id`, `_row_no` để truy vết. Nạp lại lần hai báo `already present` (nhận diện video bằng sha256), và `history bronze.raw_telemetry_a` chỉ có 1 commit (version 0, 3021 dòng): chạy lại không nhân đôi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

`stg_telemetry_b.sql` đưa B về schema của A: đổi tên cột (`spd_kmh`→`speed_mps`, `hdg`→`heading_deg`, ...), tốc độ chia 3,6 (km/h → m/s), bàn đạp chia 100 (% → 0–1), và thời gian `to_timestamp((timestamp_ms − offset_ms)/1000)`, tức epoch ms → timestamp UTC **sau khi trừ** `telemetry_clock_offset_ms` = 1500 lấy từ `bronze.video_files` (không hard-code). A chỉ cần ép kiểu. Bằng chứng: mẫu đầu tiên của B sau chỉnh là `2025-10-08 14:30:00.009` UTC, khớp giờ bắt đầu video B `14:30:00` (lệch 9 ms; nếu không trừ sẽ lệch 1,5 s). Tốc độ tối đa sau chuẩn hoá hợp lý: 71 km/h (A), 100 km/h (B).

| drive | duplicate | speed_missing | speed_out_of_range | tổng rejects | sạch | nhận về |
|---|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 53 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 53 | 3181 | 3234 |

Sạch + cách ly = nhận về ở cả hai drive (2968+53=3021, 3181+53=3234): không dòng nào biến mất. 6 dòng `speed_out_of_range` đều là 250 m/s (900 km/h). Dòng lỗi vào `silver_telemetry_rejects` kèm lý do, `_batch_id` và `_row_no` thay vì bị xoá, vì: (1) đếm được và đối soát được, ví dụ 30 dòng thiếu tốc độ/drive cho biết cảm biến có vấn đề; (2) truy ngược đúng dòng trong file nguồn để báo đội thu thập; (3) nếu luật lọc sai thì vẫn khôi phục được dữ liệu. Xoá im lặng thì số liệu "trông sạch" nhưng không ai biết đã mất gì.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` (khoảng trống > 1000 ms giữa hai mẫu) là `warn` vì mất tín hiệu là hiện tượng **có thật** của cảm biến, dữ liệu còn lại vẫn đúng và dùng được. Ở bài của tôi mỗi drive có đúng 1 khoảng trống > 1 s (A: 3073 ms, B: 3124 ms, trung vị 100 ms ≈ 10 Hz). Dừng cả pipeline vì vài giây mất sóng là quá tay; chỉ cần báo để theo dõi. Hậu quả của nó đã được xử lý ở chỗ khác: ở bước 3 tôi kiểm tra thấy đúng 6 khung/drive rơi vào khoảng trống đó (A: 08:01:59.5–08:02:02, B: 14:31:30.2–14:31:32.7, độ trễ 384→2884 ms và 612→3112 ms) bị gắn `sync_lost` (vượt `sync_tol_ms` = 150) và bị loại khỏi gold. Ngược lại `in_range` (0..70 m/s) là `error` vì tốc độ vô lý là dữ liệu **sai**: nếu lọt vào silver nó sẽ đi thẳng vào nhãn huấn luyện và mart.

Thí nghiệm `break-gate`: nới luật làm sạch `max_speed_mps` từ 70 lên 1000 → 6 dòng 250 m/s (3 mỗi drive) không còn bị cách ly mà lọt vào `silver_telemetry`. `dbt test` ra `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng **6 dòng**, khớp số dòng `speed_out_of_range` ở Câu 2. Khôi phục luật → `PASS=6 WARN=1 ERROR=0`. Điều này cho thấy cổng chất lượng là lớp phòng thủ **độc lập** với logic làm sạch: khi ai đó vô tình nới luật, test `error` vẫn chặn và các task phía sau (frames, gold) không chạy, dữ liệu xấu không đi tiếp. Test `warn` thì chỉ báo.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
