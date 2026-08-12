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
│   ├── models/         # struct request/response + models_easyjson.go (code sinh sẵn)
│   └── main.go
├── load_test/           # Công cụ kiểm thử tải bằng goroutine
│   └── main.go
├── docker-compose.yml
├── .env.example
└── README.md
```

> `pdu_backend/models/models_easyjson.go` là code marshal/unmarshal JSON do
> [easyjson](https://github.com/mailru/easyjson) sinh ra và **được commit sẵn**
> để `docker compose build` / `go build` chạy được ngay từ bản clone sạch (không
> cần bước code-gen). Khi đổi struct trong `models.go`, sinh lại bằng:
>
> ```bash
> cd pdu_backend && go generate ./...
> ```

## Kiểm thử tải

Chạy bằng Docker (cùng ràng buộc CPU với hệ thống — khuyến nghị để benchmark):

```bash
docker compose --profile manual_test run --rm load_tester
```

Hoặc chạy trực tiếp:

```bash
cd load_test
go run main.go
```

Công cụ gửi đồng thời 100 worker × 500 = 50.000 request tới Gateway, in ra tổng số request thành công/lỗi, thời gian hoàn thành và **throughput (TPS)** end-to-end (đã bao gồm ghi DB).

## Hiệu năng & tối ưu (benchmark single-core)

Hệ thống được benchmark ở chế độ **mỗi service ghim 1 core** (`cpuset` + `GOMAXPROCS=1` trong `docker-compose.yml`) nhằm đo throughput trên một lõi. Phép đo là TPS end-to-end của `load_test` (client → gateway → 3 backend → Postgres, có ghi DB).

### Kết quả (1 core/service)

| Trạng thái | TPS | Ghi chú |
|---|---|---|
| Trước tối ưu | ~5.5k | 0 lỗi |
| Sau tối ưu | ~6.1k | +10%, 0 lỗi, 50k request ghi DB đầy đủ |

**Nút cổ chai là gateway** — nó gom toàn bộ traffic và bão hoà ~100% một core, trong khi mỗi backend chỉ ~37% và Postgres ~10%. CPU profile cho thấy core chủ yếu tiêu vào **syscall I/O (~40%)** và **scheduler/timer runtime (~27%)**, còn logic routing + reverse proxy chỉ ~7%. Đây là chi phí *cấu trúc* của một reverse proxy h2c chạy trên đúng 1 lõi, không giảm thêm được bằng code.

Các tối ưu đã áp dụng cho gateway (branch `perf/single-core-15k`):

- Routing hot-path đọc trạng thái qua `atomic.Pointer` snapshot thay cho `RWMutex.RLock` mỗi request.
- Bỏ Read/Write deadline theo từng request (chỉ giữ `IdleTimeout`) để cắt timer churn của scheduler.
- Dùng **1 kết nối h2c/backend** (thay vì 8) để các HTTP/2 frame gộp chung, giảm số lần ghi socket. (Thử HTTP/1.1 cho hop này thì **chậm hơn** — mỗi request cần 1 kết nối riêng, không gộp được frame.)

### Mở rộng theo core

Cùng binary đó đạt **~14–15k TPS khi gateway được cấp 3 core** (`GOMAXPROCS=3`) — throughput scale gần tuyến tính theo số core. Vì vậy mốc **15k req/s cần cấp cho gateway nhiều hơn 1 core**; không đạt được bằng tối ưu code trong giới hạn 1 core/service.

### Sửa lỗi đi kèm

- `smContextRef` bị dính literal `%d` (còn sót sau khi bỏ `fmt.Sprintf`) → tạo ref sai kiểu `ctx-%d5`; nay dựng chuỗi bằng `strconv` (đã verify 200k bản ghi, 0 ref lỗi).
- `CREATE TABLE IF NOT EXISTS` bị race khi 3 backend khởi động cùng lúc trên DB trống (SQLSTATE 23505 trong catalog Postgres) làm 2/3 backend `log.Fatalf`; nay retry để khi một instance thắng race thì `IF NOT EXISTS` trở thành no-op.
- Data race trên `Weighted.CurrentWeight` (thuật toán Weighted Round Robin ghi state chia sẻ mà không khoá) → được serialize bằng mutex của pool.
- Repo không build được từ bản clone sạch do `models_easyjson.go` bị gitignore mà Dockerfile không có bước sinh code → nay commit file sinh sẵn + thêm `//go:generate`.

## Hạn chế đã biết

- Trên ràng buộc 1 core/service, throughput gateway trần khoảng **~6k req/s** (bão hoà bởi I/O + runtime của Go trên một lõi); muốn cao hơn phải cấp thêm core cho gateway.
- Chưa có graceful shutdown (xử lý `SIGTERM`) cho Gateway và PDU backend.
- Mới kiểm thử ở quy mô 3 PDU instance, chưa test ở quy mô lớn hơn (10-20 instance).
- README này không thay thế tài liệu thiết kế chi tiết — xem thêm báo cáo project (nếu có) để biết đầy đủ về từng thuật toán và các sự cố đã gặp trong quá trình phát triển.