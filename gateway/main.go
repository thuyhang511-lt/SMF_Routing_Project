package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy
}

type BackendPool struct {
	backends []*Backend
	counter  uint64
	mutex    sync.RWMutex
}

// Cai tien Round Robin
func (p *BackendPool) GetNextPeer() *Backend {
	p.mutex.RLock()         // Khoa Doc (Chi cho phep doc)
	defer p.mutex.RUnlock() // Mo khoa khi ham ket thuc

	total := uint64(len(p.backends))
	if total == 0 {
		return nil
	}

	start := atomic.AddUint64(&p.counter, 1)
	for i := uint64(0); i < total; i++ {
		idx := int((start + i) % total)
		if p.backends[idx].Alive {
			if i != 0 {
				atomic.StoreUint64(&p.counter, uint64(idx))
			}
			return p.backends[idx]
		}
	}
	return nil
}

func healthCheck(pool *BackendPool) {
	ticket := time.NewTicker(time.Second * 5)
	for {
		<-ticket.C

		pool.mutex.Lock() // Khoa Ghi
		for _, b := range pool.backends {
			pingURL := b.URL.String() + "/health"
			// Thu goi HTTP GET toi backend
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
				status := "SẬP (DOWN)"
				if alive {
					status = "SỐNG (UP)"
				}
				fmt.Printf("[HealthCheck] Cảnh báo: Backend %s đang %s\n", b.URL.String(), status)
			}
		}
		pool.mutex.Unlock() // Mo khoa Ghi
	}
}

func main() {
	// Khai bao danh sach 3 backend
	backendURLs := []string{
		"http://localhost:8081",
		"http://localhost:8082",
		"http://localhost:8083",
	}

	pool := &BackendPool{}

	// Khoi tao cac Backend cho vao Pool
	for _, u := range backendURLs {
		parsedUrl, _ := url.Parse(u)
		pool.backends = append(pool.backends, &Backend{
			URL:          parsedUrl,
			Alive:        true,
			ReverseProxy: httputil.NewSingleHostReverseProxy(parsedUrl),
		})
	}

	// Goroutine chay ngam Health Check
	go healthCheck(pool)

	// Ham xu ly request (Round Robin)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		peer := pool.GetNextPeer()
		if peer != nil {
			fmt.Printf("[Gateway] Forward request tới %s\n", peer.URL.String())
			peer.ReverseProxy.ServeHTTP(w, r)
			return
		}
		// Tra ve loi 503 neu khong tim thay backend nao song
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "ERROR", "cause": "NO_BACKEND_AVAILABLE"}`))
	})

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s (Round Robin + HealthCheck)\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
