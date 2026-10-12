# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Bùi Quang Vinh` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202603012` |
| GitHub | `vbwork2` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04871` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Ở Bronze, A có `ts_utc`, `speed_mps`, `heading_deg`, `lat/lon`, `gas`; B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude/longitude`, `throttle_pct`. Timestamp A là ISO UTC còn B là epoch ms, tốc độ lần lượt là m/s và km/h; dữ liệu nạp có 3.021 dòng A, 3.234 dòng B và 26 trip. Lần nạp thứ hai báo `already present`, còn `raw_telemetry_a` chỉ có Delta version 0 với 3.021 dòng. Giữ hai bảng riêng bảo toàn schema và nguồn gốc; giữ giá trị dạng chữ tránh ép kiểu sớm làm sai hoặc loại bỏ dữ liệu bẩn trước khi Silver chuẩn hóa.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging giữ A ở m/s và đổi timestamp ISO UTC sang kiểu thời gian chuẩn; B đổi `spd_kmh` sang m/s bằng cách chia 3,6, đổi epoch ms sang timestamp và trừ offset đồng hồ 1.500 ms. Bắt đầu telemetry B sau chuẩn hóa là 14:30:00.009 UTC, cách mốc video 14:30:00 đúng 9 ms. Số dòng sạch và quarantine khớp tổng nhận vào ở cả hai nguồn:

| Drive | Trùng | Thiếu tốc độ | Tốc độ ngoài miền | Dòng sạch |
|---|---:|---:|---:|---:|
| A | 20 | 30 | 3 | 2.968 |
| B | 20 | 30 | 3 | 3.181 |

106 dòng lỗi được giữ trong `silver_telemetry_rejects` cùng lý do để kiểm toán, sửa nguồn và theo dõi chất lượng; xóa im lặng sẽ làm mất dấu nguyên nhân và khiến đối soát 3.021/3.234 dòng không còn đầy đủ.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` cảnh báo khi khoảng cách mẫu vượt 1.000 ms vì mất tín hiệu cần được theo dõi nhưng không phải lúc nào cũng làm cả chuyến vô dụng; `in_range` mặc định là `error` với miền tốc độ 0–70 m/s vì giá trị ngoài miền làm dữ liệu downstream sai. Trong `break_gate.json`, khi nới luật lên 1.000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng và dbt trả `PASS=5 WARN=1 ERROR=1`; khôi phục luật gốc cho `PASS=6 WARN=1 ERROR=0`. Thí nghiệm xác nhận cổng `error` dừng dữ liệu không hợp lệ trước Gold, còn `warn` vẫn báo rủi ro cảm biến mà không chặn toàn pipeline.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Thí nghiệm trên `trip-00003` ghi nhận 1.246 dòng trước và sau hai lần chạy lại; fingerprint vẫn là `6c7480dfde7e`. Cách `append` sai tạo 120 dòng cho trip có 60 frame, tức nhân đôi đúng 2 lần. Delta `overwrite` có predicate theo trip thay thế đúng partition trong một transaction, nên khi lỗi trước commit dữ liệu cũ vẫn còn và lần chạy lại không cộng thêm dòng; xóa thủ công trước khi ghi có thể để partition trống nếu tiến trình ngắt giữa chừng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng dưới ngưỡng và gần như không có thông tin nên bị loại khỏi Gold; khung tối vẫn có nội dung, được gắn cờ nhưng giữ lại (A có 4, B có 61), phù hợp với cảnh thiếu sáng hữu ích. Mình chọn ngưỡng 2 đang dùng: nó bắt 149/602 frame ở A và 53/644 ở B, trong khi ngưỡng 6 tăng lên 363 và 244, dễ loại mất góc nhìn khác biệt. Tốc độ cao làm cảnh thay đổi nhiều hơn giữa hai frame cách 0,5 giây, nên ngưỡng quá rộng dễ gộp nhầm ảnh khác; ở tốc độ thấp, ngưỡng 2 đủ nhận ra các ảnh gần như lặp.

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
