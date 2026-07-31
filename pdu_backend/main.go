package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SNssai struct {
	Sst int    `json:"sst"`
	Sd  string `json:"sd"`
}

type CreateSessionRequest struct {
	Supi         string  `json:"supi"`
	Gpsi         string  `json:"gpsi"`
	PduSessionId int     `json:"pduSessionId"`
	Dnn          string  `json:"dnn"`
	SNssai       *SNssai `json:"sNssai"`
	ServingNfId  string  `json:"servingNfId"`
	AnType       string  `json:"anType"`
}

type CreateSessionResponse struct {
	SmContextRef string `json:"smContextRef"`
	Supi         string `json:"supi"`
	PduSessionId int    `json:"pduSessionId"`
	HandledBy    string `json:"handledBy"`
	Status       string `json:"status"`
}

var activeRequests int32
var dbPool *pgxpool.Pool

// initDB ket noi Postgres va tao bang pdu_sessions neu chua co
func initDB(dbURL string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
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

// saveSession luu session vao Postgres
func saveSession(pool *pgxpool.Pool, req CreateSessionRequest, resp CreateSessionResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	insertSQL := `
	INSERT INTO pdu_sessions
		(sm_context_ref, supi, gpsi, pdu_session_id, dnn, sst, sd, serving_nf_id, an_type, handled_by, status)
	VALUES
		($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

	_, err := pool.Exec(ctx, insertSQL,
		resp.SmContextRef,
		req.Supi,
		req.Gpsi,
		req.PduSessionId,
		req.Dnn,
		req.SNssai.Sst,
		req.SNssai.Sd,
		req.ServingNfId,
		req.AnType,
		resp.HandledBy,
		resp.Status,
	)
	return err
}

func pduSessionHandler(w http.ResponseWriter, r *http.Request, instanceName string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	atomic.AddInt32(&activeRequests, 1)
	defer atomic.AddInt32(&activeRequests, -1)

	// Parse JSON body
	var req CreateSessionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"erroe": "Invalid JSON"}`, http.StatusBadRequest)
		return
	}

	// Validate
	if req.Supi == "" || req.PduSessionId == 0 || req.Dnn == "" || req.SNssai == nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status": "ERROR", "cause": "MANDATORY_IE_MISSING"}`))
		return
	}

	// Tra response ve gateway
	response := CreateSessionResponse{
		SmContextRef: fmt.Sprintf("http://gw/nsmf-pdusession/v1/sm-contexts/ctx-%d", req.PduSessionId),
		Supi:         req.Supi,
		PduSessionId: req.PduSessionId,
		HandledBy:    instanceName,
		Status:       "ACTIVE",
	}

	// Sinh session context va luu vao database
	if dbPool != nil {
		if err := saveSession(dbPool, req, response); err != nil {
			log.Printf("[DB] Lỗi khi lưu session: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"status": "ERROR", "cause": "DB_WRITE_FAILED"}`))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func main() {
	port := flag.String("port", "8081", "Cổng để chạy server")
	flag.Parse()

	instanceName := "pdu-session-" + *port

	// Ket noi Postgres qua bien moi truong DB_URL (do docker-compose truyen vao)
	dbURL := os.Getenv("DB_URL")
	if dbURL != "" {
		pool, err := initDB(dbURL)
		if err != nil {
			log.Fatalf("[DB] Không thể khởi tạo database: %v\n", err)
		}
		defer pool.Close()
		dbPool = pool
		fmt.Println("[DB] Đã kết nối Postgres và sẵn sàng bảng pdu_sessions")
	} else {
		fmt.Println("[DB] Cảnh báo: không có DB_URL, chạy ở chế độ không lưu trữ")
	}

	http.HandleFunc("/nsmf-pdusession/v1/sm-contexts", func(w http.ResponseWriter, r *http.Request) {
		pduSessionHandler(w, r, instanceName)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	address := ":" + *port

	server := &http.Server{
		Addr:    address,
		Handler: nil,
	}

	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	fmt.Printf("PDU Backend đang chạy tại cổng%s\n", address)
	log.Fatal(server.ListenAndServe())
}
