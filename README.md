# SMF Routing Project

Mô phỏng chức năng **routing HTTP tại Gateway** của module SMF (5G Session Management Function), hỗ trợ 4 thuật toán cân bằng tải (Round Robin, Weighted Round Robin, Load-based, Maglev Consistent Hashing), health-check tự động, và lưu trữ PDU session vào PostgreSQL. Toàn bộ hệ thống chạy bằng Docker Compose.

## Kiến trúc

```
Client / AMF
     | HTTP/2 (POST /nsmf-pdusession/v1/sm-contexts)
     v
Gateway  --[thuật toán routing]-->  PDU Session #1 (pdu_1 :8081)
                                +-> PDU Session #2 (pdu_2 :8082)
                                +-> PDU Session #3 (pdu_3 :8083)
                                          |
                                          v
                                  PostgreSQL (bảng pdu_sessions)
```

| Service | Vai trò | Cổng (host) |
|---|---|---|
| `gateway` | Nhận request, health-check, áp dụng thuật toán routing, forward xuống backend | `8000` |
| `pdu_1` / `pdu_2` / `pdu_3` | Xử lý PDU Session Establishment, ghi session vào DB | `8081` / `8082` / `8083` |
| `postgres` | Lưu trữ session dùng chung cho mọi PDU instance | `5432` |
| `pgadmin` | Giao diện web quản trị Postgres | `5050` |

## Yêu cầu

- Docker & Docker Compose
- (Tuỳ chọn) Go ≥ 1.24 nếu muốn chạy/sửa code ngoài container

## Cài đặt & chạy

1. Tạo file cấu hình từ mẫu:

   ```bash
   cp .env.example .env
   ```

   Mở `.env` và đổi `POSTGRES_PASSWORD`, `PGADMIN_DEFAULT_PASSWORD` sang giá trị của riêng bạn (không dùng giá trị mẫu khi deploy thật).

2. Build và khởi động toàn bộ hệ thống:

   ```bash
   docker compose up --build
   ```

3. Kiểm tra các service đã chạy:

   | URL | Mô tả |
   |---|---|
   | http://localhost:8000 | Gateway |
   | http://localhost:5050 | pgAdmin (đăng nhập bằng `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` trong `.env`) |

   Khi thêm server trong pgAdmin, dùng **Host = `postgres`**, **Port = `5432`**, cùng user/password/db đã khai báo trong `.env`.

Ở các lần chạy sau (không sửa code), chỉ cần `docker compose up`, không cần `--build`.

## API

### Tạo PDU Session

```
POST /nsmf-pdusession/v1/sm-contexts
Content-Type: application/json
```

Request:

```json
{
  "supi": "imsi-001010000000001",
  "pduSessionId": 1,
  "dnn": "internet",
  "sNssai": { "sst": 1, "sd": "000001" },
  "servingNfId": "2ab2b5a9-68e8-4ee6-b939-024c109b520c",
  "anType": "3GPP_ACCESS"
}
```

Response `201 Created`:

```json
{
  "smContextRef": "http://gw/nsmf-pdusession/v1/sm-contexts/ctx-1",
  "supi": "imsi-001010000000001",
  "pduSessionId": 1,
  "handledBy": "pdu-session-8081",
  "status": "ACTIVE"
}
```

Các mã lỗi có thể gặp: `400 MANDATORY_IE_MISSING`, `400 INVALID_JSON`, `500 DB_WRITE_FAILED`, `503 NO_BACKEND_AVAILABLE` (khi không còn PDU instance nào sống).

### Đổi thuật toán routing (runtime, không cần restart)

```
GET /admin/algo?name=round-robin
GET /admin/algo?name=weighted
GET /admin/algo?name=load-based
GET /admin/algo?name=maglev
```

## Cấu trúc thư mục

```
SMF_Routing_Project/
├── gateway/            # Gateway: routing, health-check, reverse proxy
│   ├── algorithm/      # 4 thuật toán: round_robin, weighted_round_robin, load_based, consistent_hash (Maglev)
│   └── main.go
├── pdu_backend/         # PDU Session backend (chạy 3 instance)
│   └── main.go
├── load_test/           # Công cụ kiểm thử tải bằng goroutine
│   └── main.go
├── docker-compose.yml
├── .env.example
└── README.md
```

## Kiểm thử tải

```bash
cd load_test
go run main.go
```

Công cụ gửi đồng thời nhiều goroutine tới Gateway (mặc định 100 worker × 500 request), in ra tổng số request, tỉ lệ lỗi, độ trễ trung bình, và phân phối theo từng PDU instance — dùng để xác nhận thuật toán routing đang chọn đang hoạt động đúng.

## Hạn chế đã biết

- Chưa có graceful shutdown (xử lý `SIGTERM`) cho Gateway và PDU backend.
- Mới kiểm thử ở quy mô 3 PDU instance, chưa test ở quy mô lớn hơn (10-20 instance).
- README này không thay thế tài liệu thiết kế chi tiết — xem thêm báo cáo project (nếu có) để biết đầy đủ về từng thuật toán và các sự cố đã gặp trong quá trình phát triển.