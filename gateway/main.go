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

	"gateway/algorithm"
)

type BackendPool struct {
	backends []*algorithm.Backend
	mutex    sync.Mutex
	algo     string // "round-robin", "weighted", "load-based"

	lbState *algorithm.LoadBalancerState
	maglev  *algorithm.Maglev
}

func (p *BackendPool) buildMaglev() {

	if p.maglev == nil {
		p.maglev = &algorithm.Maglev{}
	}
	var healthy []*algorithm.Backend
	for _, b := range p.backends {
		if b.Alive {
			healthy = append(healthy, b)
		}
	}
	p.maglev.Build(healthy)
}

func (p *BackendPool) GetNextPeer(bodyBytes []byte) *algorithm.Backend {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.lbState == nil {
		p.lbState = &algorithm.LoadBalancerState{}
	}
	if p.maglev == nil {
		p.maglev = &algorithm.Maglev{}
	}

	// Loc ra danh sach cac backend con SONG
	var healthyBackends []*algorithm.Backend
	for _, b := range p.backends {
		if b.Alive {
			healthyBackends = append(healthyBackends, b)
		}
	}

	if len(healthyBackends) == 0 {
		return nil
	}

	// Chon thuat toan
	var lb algorithm.LoadBalancer
	switch p.algo {
	case "weighted":
		lb = &algorithm.Weighted{}
	case "load-based":
		lb = &algorithm.LoadBased{}
	case "maglev":
		lb = p.maglev
	default:
		lb = &algorithm.RoundRobin{State: p.lbState}
	}

	return lb.Next(healthyBackends, bodyBytes)
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
		pool.backends = append(pool.backends, &algorithm.Backend{
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
		if algo == "round-robin" || algo == "weighted" || algo == "load-based" || algo == "maglev" {
			pool.mutex.Lock()
			pool.algo = algo
			pool.mutex.Unlock()
			fmt.Printf(">> Đã đổi thuật toán sang: %s\n", algo)
			w.Write([]byte("Đã đổi thuật toán thành công:" + algo))
		} else {
			w.Write([]byte("Thuật toán không hợp lệ! Dùng: round-robin, weighted, load-based, maglev"))
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
