# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trương Thị Lan Anh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602451 |
| GitHub | SxAinsworth |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-08888 |

> Ghi chú cách tính trip: MSSV có chữ `A` nên `int(MSSV)` của GUIDE báo lỗi; tôi lấy phần chữ số `2202602451` → `2202602451 % 26 = 23` → phần tử thứ 23 của `cli list-trips` là `trip-08888`.

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

Thí nghiệm chạy lại partition `trip-00003` (60 khung). Cách của pipeline (Delta `overwrite` với predicate `trip_id = 'trip-00003'`): trước là **1246 dòng**, dấu vân tay `6c7480dfde7e`; sau **2 lần chạy lại** vẫn **1246 dòng**, dấu vân tay `6c7480dfde7e` → `unchanged: true`. Cách sai (`append`, chạy 2 lần): trip này còn **120 dòng** cho 60 khung, tức nhân đôi 2x; chạy thêm lần nữa sẽ thành 3x.

Overwrite theo partition an toàn hơn "xoá rồi ghi lại" vì Delta ghi file mới trước, rồi thay phần dữ liệu cũ của trip bằng **một commit nguyên tử** trong transaction log. Nếu task sập giữa chừng thì commit không được ghi: người đọc vẫn thấy bản cũ đầy đủ, file dở dang chỉ là rác chưa được tham chiếu. Còn xoá-rồi-ghi là hai thao tác riêng: sập sau khi xoá và trước khi ghi xong thì trip đó **mất dữ liệu** (hoặc có một nửa), và người đọc có thể thấy trạng thái trống ở giữa. Nhờ vậy Airflow có thể retry/backfill một trip bất kỳ mà không cần dọn dẹp tay, và lịch sử commit vẫn cho time travel về bản trước.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

(drive A có 602 khung, B có 644; ngưỡng 1/4/8: A 109/269/438, B 23/137/348.)

**Đen và tối**: khung **đen** có độ sáng trung bình < 12 (`dark_luma`). Ở bài của tôi độ sáng trung bình của khung đen là 0,0, tức là lỗi camera/mất tín hiệu, không chứa thông tin, nên bị loại khỏi gold (A: 6, B: 4). Khung **tối** có độ sáng 12–40 (`dark_soft`, trung bình 37,4 ở A và 30,8 ở B, so với 104/88 ở khung thường), là cảnh thật như đường hầm, bóng râm, nên chỉ gắn cờ `is_dark` và **giữ lại** (A: 4, B: 61). Tôi đồng ý: mô hình xe tự lái phải học cả điều kiện thiếu sáng; bỏ khung tối sẽ làm dataset lệch về ban ngày và mô hình kém đúng ở chỗ khó nhất. Cờ vẫn có để lọc hoặc cân bằng lại khi cần.

**Ngưỡng trùng**: ngưỡng 0 chỉ bắt khung giống hệt (A: 61, gần như toàn là đoạn camera đứng hình); ngưỡng 6 coi 363/602 khung của A (60%) và 244/644 của B là trùng, sẽ loại cả khung khác nhau thật. Tôi giữ ngưỡng **2**: bắt được khung đứng hình (11/12 khung đóng băng ở A bị bắt) mà chỉ loại 25% khung A và 8% khung B. Ngưỡng phụ thuộc tốc độ: ở 2 khung/giây, xe càng chậm thì hai khung liền nhau càng giống. Số liệu của tôi với ngưỡng 2: drive A khi xe < 2 m/s có **59,3%** khung bị coi là trùng, còn ở 2–8 m/s chỉ 11,3%; drive B (chạy nhanh, phần lớn ≥ 15 m/s) chỉ 9,9%. Vì vậy ngưỡng cố định sẽ loại quá nhiều khi kẹt xe hoặc dừng đèn đỏ. Cách tốt hơn là cho ngưỡng phụ thuộc tốc độ, hoặc lấy mẫu theo quãng đường thay vì theo thời gian.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong `leakage.json`: có 1019 khung đáng dùng thuộc 26 trip. Chia **theo khung hình** (ngẫu nhiên 70/15/15) thì **26/26 trip** bị chia cắt qua nhiều split, và cả **26 trip** đều có khung ở cả train lẫn test. Chia **theo trip** (gold của pipeline) thì **0 trip** bị chia cắt: 18 trip train, 4 val, 4 test, và test `no trip is in two splits` của verify đạt.

Ở 2 khung/giây, hai khung liền nhau của cùng trip gần như giống hệt: cùng con đường, cùng ánh sáng, thời tiết, cùng xe phía trước (riêng drive A đã có 149 cặp liền nhau gần trùng ở ngưỡng dHash 2). Nếu khung t nằm ở train và khung t+0,5 s nằm ở test, mô hình chỉ cần "nhớ" cảnh là trả lời đúng, nên điểm test đo khả năng ghi nhớ chứ không phải khả năng tổng quát hoá sang cảnh mới. Khi triển khai trên con đường chưa từng thấy, độ chính xác sẽ tụt so với con số đã báo cáo. Chia theo trip buộc test là các cảnh mô hình chưa thấy, giống điều kiện thật.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` của tôi là **`f7bbaac3feee`** = 12 ký tự đầu của sha256 trên danh sách `frame_id|split` (sắp theo `frame_id`, trong `pipeline/publish.py`). Nó là **băm nội dung** nên trả lời đúng câu hỏi "hai dataset có giống nhau không": cùng khung, cùng split thì cùng version, dù chạy lúc nào, trên máy nào; chỉ cần một khung đổi split hoặc bị thêm/bớt là version đổi. Ngày giờ hoặc số đếm thì đổi sau mỗi lần chạy dù nội dung không đổi (một mô hình train trên "v5" không cho biết v5 chứa gì), và hai máy có thể đặt cùng số cho hai nội dung khác nhau.

Hai lần chạy liên tiếp: `step 4` rồi `cli export-dataset` đều ra `f7bbaac3feee`, 1019 khung (756/123/140) và cùng `manifest_sha256` `6a7f4db7…e894`; trùng cả với lần `step all` đầu tiên. Bản của tôi khác bạn bè vì `gold_dataset_frames.sql` xếp trip trong mỗi drive theo `md5(trip_id || LAB_STUDENT_ID)`: MSSV `2A202602451` là "muối" làm đổi thứ tự trip, do đó đổi trip nào rơi vào train/val/test → số khung mỗi split khác (của tôi: A 338/61/37, B 418/62/103) → danh sách `frame_id|split` khác → hash khác. Dữ liệu và luật lọc giống hệt nên tổng 1019 khung vẫn như nhau, chỉ cách chia khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

`lake/gold/datasets/f7bbaac3feee/manifest.json` trả lời: dataset này được tạo từ hai video gốc có sha256 `636ba1a9…e9aa` (drive_a) và `bfa62703…9fdc` (drive_b), qua các bảng Delta ở đúng phiên bản: `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1, `silver.telemetry` v0, `silver.frames` **v25**, `silver.frames_flagged` v0, `silver.frame_telemetry` v0 (manifest của lần chạy cuối `step all --reset`). `silver.frames` ở v25 vì mỗi trip là một commit overwrite theo partition: 26 trip → v0–v25. Lúc làm bước 4 (trước khi reset) tôi đọc được v27, vì thêm 2 lần chạy lại `trip-00003` của thí nghiệm idempotency (`history` xác nhận). Hai manifest khác số phiên bản bảng nhưng cùng `dataset_version` `f7bbaac3feee` và cùng `manifest_sha256` `6a7f4db7…e894`, vì version chỉ phụ thuộc nội dung. Manifest còn ghi luật: loại `is_black, is_blurry, is_near_dup, not sync_ok`; split "per trip, stratified by drive, md5 order"; kết quả 1019 khung (A 436, B 583), 756/123/140, 18/4/4 trip.

3 tháng sau, để tái tạo đúng dataset cần: (1) sha256 video để kiểm tra đúng file gốc; (2) số phiên bản từng bảng Delta để time travel về đúng trạng thái đó, kể cả khi bảng đã được ghi thêm; (3) luật lọc, luật chia và MSSV (muối của split); (4) `dataset_version` và `manifest_sha256` để kiểm chứng kết quả tái tạo trùng khớp. Manifest **chưa** ghi phiên bản mã/dbt (commit git) và các ngưỡng (`dup_hamming` = 2, `sync_tol_ms` = 150...), nên tôi sẽ thêm commit hash của repo vào lineage.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Đặt `LAB_FAIL_ONCE=trip-08888`, trigger `day09_reset` rồi `day09_video_pipeline` (run `crash_run`): đúng **1 task đỏ** là `silver_frames` map index 23; log ghi `RuntimeError: simulated crash while extracting trip-08888; run it again` (`fail_task_log.png`). **7 task `upstream_failed`**: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse` (`fail_graph.png`). 29 task còn lại xanh: 25 `silver_frames` khác + 3 bronze + `trip_commands`.

Tôi chỉ clear task đỏ kèm downstream (`--only-failed --downstream`): đúng **8 task chạy lại** (silver_frames[23] + 7 task sau), 29 task đã xanh **không** chạy lại; run chuyển `success`, 37/37 task xanh (`airflow_retry_graph.png`). Sau đó `verify` đạt **27/27**. Không nhân đôi: `silver.frames` có 1246 dòng = 1246 `frame_id` khác nhau, và `history silver.frames` chỉ có **một** commit cho `trip-08888` (v25, 60 dòng), vì lần sập xảy ra trước khi ghi, còn lần chạy lại ghi bằng overwrite theo partition. `dataset_version` **không đổi**: vẫn `f7bbaac3feee`, 756/123/140. Đây là lợi ích của việc chia `silver_frames` thành 26 task theo trip: hỏng một trip chỉ tốn chạy lại một trip.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Hiện 2 drive (~10 phút video, 1246 khung) chạy mất khoảng 2 phút trên một container. 500 xe/ngày, mỗi xe vài giờ, 7 camera, là gấp hàng chục nghìn lần.
1. **Giải mã video (FFmpeg trong một container Airflow)** vỡ đầu tiên: decode là nút cổ chai CPU (slide 24), và `silver_frames` đang chạy tuần tự (`max_active_tis_per_dagrun=1`, `PARALLELISM=2`), khoảng 3 s/trip trong log của tôi. Thay bằng hàng đợi + cụm worker tự co giãn (Kubernetes/Ray/Spark), decode bằng GPU (NVDEC), xử lý ngay khi file về (sự kiện từ object storage) thay vì theo lô.
2. **DuckDB + dbt trên một máy** (slide 22): bảng Delta đọc/ghi từ một tiến trình, toàn bộ ASOF join nằm trong RAM. Thay bằng engine phân tán (Spark/Trino trên Delta hoặc Iceberg), partition theo ngày/xe, xử lý tăng dần thay vì chạy lại toàn bộ.
3. **Airflow với 1 task/trip, LocalExecutor, 1 container**: hàng chục nghìn trip/ngày sẽ làm scheduler và metadata DB quá tải. Chuyển sang CeleryExecutor/KubernetesExecutor, gom trip thành lô, hoặc dùng công cụ streaming (Kafka → Flink) cho telemetry (slide 23). Ngoài ra MinIO một node cần thành cluster/S3, và Postgres + Metabase cần warehouse cột (BigQuery/ClickHouse).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: **trip nào bị loại nhiều khung nhất, và vì sao?** (card đầu tiên trong `metabase_dashboard.png`)

```sql
select trip_id, drive_id, split, lighting, avg_speed_kmh,
       frames, frames_kept, frames - frames_kept as frames_dropped,
       round(100.0 * (frames - frames_kept) / frames, 1) as pct_dropped
from marts.mart_trip_summary
order by pct_dropped desc
limit 10
```

Top 5 đều thuộc drive_a: `trip-04622` 56,7% (34/60), `trip-03904` 50%, `trip-02086` 46,7%, `trip-08888` 40%, `trip-07774` 38,2%. Gộp theo tốc độ trung bình của trip: < 20 km/h bị loại 32,4% khung, 20–50 km/h 17,1%, ≥ 50 km/h 14,4%, khớp với Câu 5: xe chậm thì hai khung liền nhau giống nhau. Ngoại lệ đáng chú ý là `trip-04622` chạy **67 km/h** mà bị loại nhiều nhất. Tra silver cho thấy cả 34 khung bị loại đều do `is_near_dup` (không có khung đen/mờ/mất đồng bộ). Tôi mở hai ảnh `02401.jpg` và `02411.jpg`: đó là đường thẳng có tường cao hai bên, bám theo cùng một xe phía trước, nên ảnh gần như không đổi dù xe đang chạy. Hai trip `test` của drive_a cũng nằm trong top 2, nên test A chỉ còn 37 khung. Điều này cho thấy ngưỡng dHash cố định có thể làm mỏng tập test.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
