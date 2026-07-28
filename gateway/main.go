package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"
	"hash/crc32"
	"hash/fnv"
	"strconv"
)

const M = 1009

type Backend struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy

	// Thuat toan Weighted Round Robin
	Weight        int
	CurrentWeight int

	// Thuat toan Load-based
	ActiveRequests int32
}

type BackendPool struct {
	backends []*Backend
	counter  uint64
	mutex    sync.Mutex
	algo     string // "round-robin", "weighted", "least-load"

	maglevD        []int
	maglevBackends []*Backend
}

func hash1(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func hash2(s string) int {
	return int(crc32.ChecksumIEEE([]byte(s)))
}

func (p *BackendPool) buildMaglev() {
	var healthy []*Backend
	for _, b := range p.backends {
		if b.Alive {
			healthy = append(healthy, b)
		}
	}

	N := len(healthy)
	if N == 0 {
		p.maglevD = nil
		p.maglevBackends = nil
		return
	}

	T := make([][]int, N)
	for i, b := range healthy {
		name := b.URL.String()
		offset := hash1(name) % M
		skip := (hash2(name) % (M - 1)) + 1

		T[i] = make([]int, M)
		for j := 0; j < M; j++ {
			T[i][j] = (offset + j*skip) % M
		}
	}

	D := make([]int, M)
	for i := range D {
		D[i] = -1
	}

	next := make([]int, N)
	filled := 0

	for filled < M {
		for i := 0; i < N; i++ {
			for next[i] < M {
				b := T[i][next[i]]
				next[i]++
				if D[b] == -1 {
					D[b] = i
					filled++
					break
				}
			}
		}
	}

	p.maglevD = D
	p.maglevBackends = healthy
}

func (p *BackendPool) GetNextPeer(bodyBytes []byte) *Backend {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Loc ra danh sach cac backend con SONG
	var healthyBackends []*Backend
	for _, b := range p.backends {
		if b.Alive {
			healthyBackends = append(healthyBackends, b)
		}
	}

	if len(healthyBackends) == 0 {
		return nil
	}

	// Chon thuat toan
	switch p.algo {
	case "weighted":
		return p.getWeighted(healthyBackends)
	case "least-load":
		return p.getLeaseLoad(healthyBackends)
	case "maglev":
		return p.getMaglev(bodyBytes)
	default:
		return p.getRoundRobin(healthyBackends)
	}
}

// ROUND ROBIN
func (p *BackendPool) getRoundRobin(healthy []*Backend) *Backend {
	next := atomic.AddUint64(&p.counter, 1)
	idx := int((next - 1) % uint64(len(healthy)))
	return healthy[idx]
}

// WEIGHTED ROUND ROBIN
func (p *BackendPool) getWeighted(healthy []*Backend) *Backend {
	var best *Backend
	totalWeight := 0

	for _, b := range healthy {
		b.CurrentWeight += b.Weight
		totalWeight += b.Weight

		// Chon backend co CurrentWeight lon nhat
		if best == nil || b.CurrentWeight > best.CurrentWeight {
			best = b
		}
	}

	// Giam CurrentWeight cua backend duoc chon
	if best != nil {
		best.CurrentWeight -= totalWeight
	}
	return best
}

// LEAST LOAD (LOAD-BASED)
func (p *BackendPool) getLeaseLoad(healthy []*Backend) *Backend {
	var best *Backend

	for _, b := range healthy {
		if best == nil || atomic.LoadInt32(&b.ActiveRequests) < atomic.LoadInt32(&best.ActiveRequests) {
			best = b
		}
	}
	return best
}

// MAGLEV HASHING
func (p *BackendPool) getMaglev(bodyBytes []byte) *Backend {
	if len(p.maglevD) == 0 || len(p.maglevBackends) == 0 {
		return nil
	}

	// Đọc supi từ JSON
	var payload struct {
		Supi string `json:"supi"`
	}
	json.Unmarshal(bodyBytes, &payload)
	supi := payload.Supi

	bucketId := 0
	if len(supi) >= 3 {
		last3 := supi[len(supi)-3:]
		val, err := strconv.Atoi(last3)
		if err == nil {
			bucketId = val % M
		} else {
			bucketId = hash1(supi) % M
		}
	} else {
		bucketId = hash1(supi) % M
	}

	pduIndex := p.maglevD[bucketId]
	if pduIndex >= 0 && pduIndex < len(p.maglevBackends) {
		return p.maglevBackends[pduIndex]
	}
	return nil
}

func healthCheck(pool *BackendPool) {
	ticket := time.NewTicker(time.Second * 5)
	for {
		<-ticket.C
		changed := false

		pool.mutex.Lock() // Khoa Ghi
		for _, b := range pool.backends {
			pingURL := b.URL.String() + "/health"
			// Goi HTTP GET toi backend
			resp, err := http.Get(pingURL)

			alive := false
			if err == nil && resp.StatusCode == http.StatusOK {
				alive = true
			}
			if resp != nil {
				resp.Body.Close()
			}

			// In log neu trang thai thay doi
			if b.Alive != alive {
				b.Alive = alive
				changed = true
				status := "SẬP (DOWN)"
				if alive {
					status = "SỐNG (UP)"
				}
				fmt.Printf("[HealthCheck] Cảnh báo: Backend %s đang %s\n", b.URL.String(), status)
			}
		}

		if changed {
			fmt.Println("[Maglev] Phát hiện thay đổi, đang xây dựng lại bảng Hash...")
			pool.buildMaglev()
		}
		pool.mutex.Unlock() // Mo khoa Ghi
	}
}

func main() {
	// Khoi tao danh sach 3 backend co gan san Trong so
	// 8081 mạnh nhất (Weight=3), 8082 vừa (2), 8083 yếu nhất (1)
	configs := []struct {
		url    string
		weight int
	}{
		{"http://localhost:8081", 3},
		{"http://localhost:8082", 2},
		{"http://localhost:8083", 1},
	}

	pool := &BackendPool{
		algo: "round-robin",
	}

	// Khoi tao cac Backend cho vao Pool
	for _, cfg := range configs {
		parsedUrl, _ := url.Parse(cfg.url)
		pool.backends = append(pool.backends, &Backend{
			URL:          parsedUrl,
			Alive:        true,
			Weight:       cfg.weight,
			ReverseProxy: httputil.NewSingleHostReverseProxy(parsedUrl),
		})
	}

	// Khoi tao bang Maglev
	pool.buildMaglev()

	// Goroutine chay ngam Health Check
	go healthCheck(pool)

	// API doi thuat toan
	http.HandleFunc("/admin/algo", func(w http.ResponseWriter, r *http.Request) {
		algo := r.URL.Query().Get("name")
		if algo == "round-robin" || algo == "weighted" || algo == "least-load" || algo == "maglev" {
			pool.mutex.Lock()
			pool.algo = algo
			pool.mutex.Unlock()
			fmt.Printf(">> Đã đổi thuật toán sang: %s\n", algo)
			w.Write([]byte("Đã đổi thuật toán thành công:" + algo))
		} else {
			w.Write([]byte("Thuật toán không hợp lệ! Dùng: round-robin, weighted, least-load"))
		}
	})

	// API Routing chinh
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Doc Body de lay SUPI
		bodyBytes, err := io.ReadAll(r.Body)
		if err == nil {
			// Khoi phuc lai Body de Reverse Proxy forward tiep di
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		peer := pool.GetNextPeer(bodyBytes)
		if peer != nil {
			// TANG bien dem tai (Load) truoc khi forward
			atomic.AddInt32(&peer.ActiveRequests, 1)

			fmt.Printf("[Gateway] Algo: %s | Forward request tới %s\n", pool.algo, peer.URL.String())
			peer.ReverseProxy.ServeHTTP(w, r)

			// GIAM bien dem tai sau khi xu ly xong
			atomic.AddInt32(&peer.ActiveRequests, -1)
			return
		}
		// Tra ve loi 503 neu khong tim thay backend nao song
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "ERROR", "cause": "NO_BACKEND_AVAILABLE"}`))
	})

	// Khoi tao HTTP/2 Cleartext Server (theo chuan 3GPP)
	server := &http.Server{
		Addr:    ":8000",
		Handler: handler,
	}

	// Ho tro HTTP/2 khong ma hoa (h2c) truc tiep tu thu vien chuan
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s\n", port)
	log.Fatal(server.ListenAndServe())
}
