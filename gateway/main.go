package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"gateway/algorithm"
)

type TransportPool struct {
	transports []http.RoundTripper
	counter    uint64
}

func (tp *TransportPool) NextTransport() http.RoundTripper {
	next := atomic.AddUint64(&tp.counter, 1)
	idx := int((next - 1) % uint64(len(tp.transports)))
	return tp.transports[idx]
}

func (tp *TransportPool) RoundTrip(req *http.Request) (*http.Response, error) {
	// Lay 1 transport ranh roi tu Pool de xu ly request nay
	transport := tp.NextTransport()
	return transport.RoundTrip(req)
}

// Ham khoi tao Pool gom 8 Transport (Client) cho HTTP/2
func NewTransportPool(poolSize int) *TransportPool {
	tp := &TransportPool{
		transports: make([]http.RoundTripper, poolSize),
	}

	for i := 0; i < poolSize; i++ {
		tp.transports[i] = &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
			// ReadIdleTimeout: time.Second * 30,
			// PingTimeout:     time.Second * 15,
		}
	}
	return tp
}

type BackendPool struct {
	backends []*algorithm.Backend
	mutex    sync.RWMutex
	algo     string // "round-robin", "weighted", "load-based"

	lbState *algorithm.LoadBalancerState
	maglev  *algorithm.Maglev

	registry map[string]time.Time // ghi thoi gian song cuoi cung cua cac PDU
}

type HeartbeatMsg struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

var healthCheckClient = &http.Client{
	Timeout: 2 * time.Second,
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
	p.mutex.RLock()
	defer p.mutex.RUnlock()

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

func main() {

	// Khoi tao BackendPool trong
	pool := &BackendPool{
		backends: make([]*algorithm.Backend, 0),
		algo:     "round-robin",
		lbState:  &algorithm.LoadBalancerState{},
		maglev:   &algorithm.Maglev{},
		registry: make(map[string]time.Time),
	}

	// Khoi tao 1 pool gom 8 ket noi
	sharedTransportPool := NewTransportPool(8)
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		var msg HeartbeatMsg
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		pool.mutex.Lock()
		defer pool.mutex.Unlock()

		pool.registry[msg.ID] = time.Now()

		exists := false
		for _, b := range pool.backends {
			if b.ID == msg.ID {
				if !b.Alive {
					fmt.Printf("[Gateway] PDU %s đã SỐNG LẠI tại %s\n", msg.ID, msg.URL)
				}
				b.Alive = true
				exists = true
				break
			}
		}

		if !exists {
			parsedUrl, _ := url.Parse(msg.URL)
			proxy := httputil.NewSingleHostReverseProxy(parsedUrl)
			proxy.Transport = sharedTransportPool

			newBackend := &algorithm.Backend{
				ID:           msg.ID,
				URL:          parsedUrl,
				ReverseProxy: proxy,
				Weight:       1,
				Alive:        true,
			}
			pool.backends = append(pool.backends, newBackend)
			fmt.Printf("[Gateway] Phát hiện PDU MỚI gia nhập vòng: %s (%s)\n", msg.ID, msg.URL)

			if pool.algo == "maglev" {
				pool.buildMaglev()
			}
		}
		w.WriteHeader(http.StatusOK)
	})

	go func() {
		for {
			time.Sleep(5 * time.Second)
			pool.mutex.Lock()

			changed := false
			now := time.Now()
			for _, b := range pool.backends {
				lastSeen, ok := pool.registry[b.ID]

				if ok && now.Sub(lastSeen) > 10*time.Second {
					if b.Alive {
						b.Alive = false
						changed = true
						fmt.Printf("[Gateway] Cảnh báo: %s ĐÃ CHẾT (Mất kết nối)\n", b.ID)
					}
				}
			}

			if changed && pool.algo == "maglev" {
				pool.buildMaglev()
			}
			pool.mutex.Unlock()
		}
	}()

	// API doi thuat toan
	mux.HandleFunc("/admin/algo", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Doc Body de lay SUPI
		var bodyBytes []byte

		pool.mutex.RLock()
		currentAlgo := pool.algo
		pool.mutex.RUnlock()

		if currentAlgo == "maglev" {
			var err error
			bodyBytes, err = io.ReadAll(r.Body)
			if err == nil {
				// Khoi phuc lai Body de Reverse Proxy forward tiep di
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		}

		peer := pool.GetNextPeer(bodyBytes)
		if peer != nil {
			// TANG bien dem tai (Load) truoc khi forward
			atomic.AddInt32(&peer.ActiveRequests, 1)

			// fmt.Printf("[Gateway] Algo: %s | Forward request tới %s\n", pool.algo, peer.URL.String())
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
		Handler: mux,
	}

	// Ho tro HTTP/2 khong ma hoa (h2c) truc tiep tu thu vien chuan
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s\n", port)
	log.Fatal(server.ListenAndServe())
}
