package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"pdu_backend/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mailru/easyjson"
)

type SessionRecord struct {
	Req  models.CreateSessionRequest
	Resp models.CreateSessionResponse
}

var sessionQueue = make(chan SessionRecord, 50000)
var activeRequests int32
var dbPool *pgxpool.Pool

var pduBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4096)
		return &buf
	},
}

// initDB ket noi Postgres va tao bang pdu_sessions neu chua co
func initDB(dbURL string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("khong the doc DB_URL: %w", err)
	}

	// Gioi han so connection cho MOI instance pdu_backend.
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("khong the tao connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("khong the ket noi Postgres: %w", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS pdu_sessions (
		id              SERIAL PRIMARY KEY,
		sm_context_ref  TEXT NOT NULL,
		supi            TEXT NOT NULL,
		gpsi            TEXT,
		pdu_session_id  INT NOT NULL,
		dnn             TEXT NOT NULL,
		sst             INT,
		sd              TEXT,
		serving_nf_id   TEXT,
		an_type         TEXT,
		handled_by      TEXT NOT NULL,
		status          TEXT NOT NULL,
		created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
	);`

	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("khong the tao bang pdu_sessions: %w", err)
	}

	return pool, nil
}

func startDBWorker(pool *pgxpool.Pool, batchSize int, flushInterval time.Duration, workerID int) {
	go func() {
		var batch *pgx.Batch
		batch = &pgx.Batch{}

		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()

		flush := func() {
			if batch.Len() == 0 {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			br := pool.SendBatch(ctx, batch)

			var errCount int
			for i := 0; i < batch.Len(); i++ {
				_, err := br.Exec()
				if err != nil {
					errCount++
				}
			}
			br.Close() // dong BatchResults de tra Connection lai Pool

			if errCount > 0 {
				log.Printf("[Worker %d] Ghi Batch xong nhưng có %d lỗi\n", workerID, errCount)
			}

			batch = &pgx.Batch{}
		}

		for {
			select {
			case record := <-sessionQueue:
				query := `INSERT INTO pdu_sessions 
                    (sm_context_ref, supi, gpsi, pdu_session_id, dnn, sst, sd, serving_nf_id, an_type, handled_by, status) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

				batch.Queue(query,
					record.Resp.SmContextRef, record.Req.Supi, record.Req.Gpsi,
					record.Req.PduSessionId, record.Req.Dnn, record.Req.SNssai.Sst,
					record.Req.SNssai.Sd, record.Req.ServingNfId, record.Req.AnType,
					record.Resp.HandledBy, record.Resp.Status,
				)

				if batch.Len() >= batchSize {
					flush()
				}

			case <-ticker.C:
				flush()
			}
		}
	}()
}

func pduSessionHandler(w http.ResponseWriter, r *http.Request, instanceName string) {
	if r.Method != http.MethodPost {
		// http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// atomic.AddInt32(&activeRequests, 1)
	// defer atomic.AddInt32(&activeRequests, -1)

	bufPtr := pduBufferPool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:cap(buf)]
	defer pduBufferPool.Put(bufPtr)

	n, err := r.Body.Read(buf)
	if err != nil && err != io.EOF {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status": "ERROR", "cause": "READ_BODY_FAILED"}`))
		return
	}
	bodyBytes := buf[:n]

	var req models.CreateSessionRequest
	if err := req.UnmarshalJSON(bodyBytes); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status": "ERROR", "cause": "INVALID_JSON_FORMAT"}`))
		return
	}

	// Validate
	if req.Supi == "" || req.PduSessionId == 0 || req.Dnn == "" || req.SNssai == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status": "ERROR", "cause": "MANDATORY_IE_MISSING"}`))
		return
	}

	// Tra response ve gateway
	response := models.CreateSessionResponse{
		SmContextRef: "http://gw/nsmf-pdusession/v1/sm-contexts/ctx-%d" + strconv.Itoa(req.PduSessionId), // fmt.Sprintf("http://gw/nsmf-pdusession/v1/sm-contexts/ctx-%d", req.PduSessionId),
		Supi:         req.Supi,
		PduSessionId: req.PduSessionId,
		HandledBy:    instanceName,
		Status:       "ACTIVE",
	}

	// Sinh session context va luu vao database
	if dbPool != nil {
		select {
		case sessionQueue <- SessionRecord{Req: req, Resp: response}:

		default:
			log.Println("[CẢNH BÁO] Hệ thống quá tải, rớt gói tin DB!")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"status": "ERROR", "cause": "SERVER_OVERLOADED"}`))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = easyjson.MarshalToWriter(&response, w)
}

func main() {
	port := flag.String("port", "0", "Cổng để chạy server")
	flag.Parse()

	instanceName := "pdu-session-" + *port

	// Ket noi Postgres qua bien moi truong DB_URL (do docker-compose truyen vao)
	dbURL := os.Getenv("DB_URL")
	if dbURL != "" {
		pool, err := initDB(dbURL)
		if err != nil {
			log.Fatalf("[DB] Không thể khởi tạo database: %v\n", err)
		}
		// defer pool.Close()
		dbPool = pool
		fmt.Println("[DB] Đã kết nối Postgres và sẵn sàng bảng pdu_sessions")

		numWorkers := 2
		for i := 1; i <= numWorkers; i++ {
			startDBWorker(dbPool, 1000, 500*time.Millisecond, i)
		}
		fmt.Printf("[DB] Đã khởi chạy %d DB Workers xử lý Asynchronous Write\n", numWorkers)
	} else {
		fmt.Println("[DB] Cảnh báo: không có DB_URL, chạy ở chế độ không lưu trữ")
	}

	// DYNAMIC PORT
	address := ":" + *port
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Lỗi tạo listener: %v", err)
	}

	actualPort := listener.Addr().(*net.TCPAddr).Port
	hostname, _ := os.Hostname()

	instanceID := fmt.Sprintf("pdu-%s-%d", hostname, actualPort)
	myURL := fmt.Sprintf("http://%s:%d", hostname, actualPort)

	// HEARTBEAT
	gatewayURL := os.Getenv("GATEWAY_URL")
	if gatewayURL != "" {
		go func() {
			client := &http.Client{Timeout: 2 * time.Second}
			for {
				msg := models.HeartbeatMsg{ID: instanceID, URL: myURL}
				body, err := msg.MarshalJSON()
				if err == nil {
					_, err := client.Post(gatewayURL+"/admin/heartbeat", "application/json", bytes.NewBuffer(body))
					if err != nil {
						log.Printf("[%s] Lỗi gửi Heartbeat tới Gateway: %v\n", instanceID, err)
					}
				}

				time.Sleep(3 * time.Second)
			}
		}()
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/nsmf-pdusession/v1/sm-contexts", func(w http.ResponseWriter, r *http.Request) {
		pduSessionHandler(w, r, instanceName)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	fmt.Printf("PDU Backend (Instance: %s) đang lắng nghe kết nối tại cổng %d\n", instanceID, actualPort)
	log.Fatal(server.Serve(listener))
}
