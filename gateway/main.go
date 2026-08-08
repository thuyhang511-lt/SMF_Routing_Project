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
	idx := int((next - 1) & uint64(len(tp.transports)-1))
	return tp.transports[idx]
}

func (tp *TransportPool) RoundTrip(req *http.Request) (*http.Response, error) {
	// Lay 1 transport ranh roi tu Pool de xu ly request nay
	transport := tp.NextTransport()
	return transport.RoundTrip(req)
}

// Ham khoi tao Pool gom 8 Transport (Client) cho HTTP/2
func NewTransportPool(poolSize int) *TransportPool {
	isPowerOfTwo := (poolSize > 0) && ((poolSize & (poolSize - 1)) == 0)
	if !isPowerOfTwo {
		log.Fatalf("Lỗi: Kích thước TransportPool (%d) BẮT BUỘC phải là lũy thừa của 2 (ví dụ: 2, 4, 8, 16) để tối ưu hóa CPU.", poolSize)
	}
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

type RoutingState struct {
	Algo            string
	HealthyBackends []*algorithm.Backend
	Maglev          *algorithm.Maglev
	LBState         *algorithm.LoadBalancerState
}

type BackendPool struct {
	state atomic.Value

	writerMutex sync.Mutex
	backends    []*algorithm.Backend
	registry    map[string]time.Time // ghi thoi gian song cuoi cung cua cac PDU
}

type HeartbeatMsg struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

var healthCheckClient = &http.Client{
	Timeout: 2 * time.Second,
}

func (p *BackendPool) rebuildRoutingState(newAlgo string) {
	// 1. Load state hiện tại (để lấy LBState cũ, giữ nguyên biến đếm của Round-Robin)
	oldState := p.state.Load().(*RoutingState)

	algo := oldState.Algo
	if newAlgo != "" {
		algo = newAlgo
	}

	// 2. Lọc ra danh sách các PDU còn sống
	var healthy []*algorithm.Backend
	for _, b := range p.backends {
		if b.Alive {
			healthy = append(healthy, b)
		}
	}

	// 3. Chuẩn bị state mới
	newState := &RoutingState{
		Algo:            algo,
		HealthyBackends: healthy,
		LBState:         oldState.LBState, // Giữ nguyên state của thuật toán cũ
		Maglev:          oldState.Maglev,  // Tạm giữ bảng băm cũ
	}

	// 4. SHADOW TABLE: Tính toán Maglev trên RAM ảo (Không hề chặn luồng đọc)
	if algo == "maglev" {
		newMaglev := &algorithm.Maglev{}
		newMaglev.Build(healthy) // Thao tác tốn CPU nhất giờ được chạy độc lập
		newState.Maglev = newMaglev
	}

	// 5. TRÁO ĐỔI TRẠNG THÁI (Zero-lock swap)
	p.state.Store(newState)
}

func (p *BackendPool) GetNextPeer(bodyBytes []byte) *algorithm.Backend {
	// Đọc thẳng state từ RAM bằng Atomic (nhanh tương đương tốc độ ánh sáng)
	state := p.state.Load().(*RoutingState)

	if len(state.HealthyBackends) == 0 {
		return nil
	}

	var lb algorithm.LoadBalancer
	switch state.Algo {
	case "weighted":
		lb = &algorithm.Weighted{}
	case "load-based":
		lb = &algorithm.LoadBased{}
	case "maglev":
		lb = state.Maglev
	default:
		lb = &algorithm.RoundRobin{State: state.LBState}
	}

	return lb.Next(state.HealthyBackends, bodyBytes)
}

func main() {

	// Khoi tao BackendPool trong
	pool := &BackendPool{
		backends: make([]*algorithm.Backend, 0),
		registry: make(map[string]time.Time),
	}

	initialState := &RoutingState{
		Algo:            "round-robin",
		HealthyBackends: make([]*algorithm.Backend, 0),
		Maglev:          &algorithm.Maglev{},
		LBState:         &algorithm.LoadBalancerState{},
	}
	pool.state.Store(initialState)

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

		pool.writerMutex.Lock()
		defer pool.writerMutex.Unlock()

		pool.registry[msg.ID] = time.Now()

		exists := false
		changed := false // Biến cờ đánh dấu cần build lại state

		for _, b := range pool.backends {
			if b.ID == msg.ID {
				if !b.Alive {
					fmt.Printf("[Gateway] PDU %s đã SỐNG LẠI tại %s\n", msg.ID, msg.URL)
					b.Alive = true
					changed = true // Trạng thái thay đổi (chết -> sống)
				}
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
			changed = true // Trạng thái thay đổi (thêm mới)
		}

		// TÍNH TOÁN LẠI ROUTING STATE NẾU CÓ THAY ĐỔI
		if changed {
			pool.rebuildRoutingState("") // Truyền "" để giữ nguyên thuật toán hiện tại
		}

		w.WriteHeader(http.StatusOK)
	})

	go func() {
		for {
			time.Sleep(5 * time.Second)

			pool.writerMutex.Lock()
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

			if changed {
				pool.rebuildRoutingState("") // Loại PDU chết khỏi RoutingState
			}
			pool.writerMutex.Unlock()
		}
	}()

	// API doi thuat toan
	mux.HandleFunc("/admin/algo", func(w http.ResponseWriter, r *http.Request) {
		algo := r.URL.Query().Get("name")
		if algo == "round-robin" || algo == "weighted" || algo == "load-based" || algo == "maglev" {
			pool.writerMutex.Lock()
			pool.rebuildRoutingState(algo) // Tự động tính toán lại
			pool.writerMutex.Unlock()

			fmt.Printf(">> Đã đổi thuật toán sang: %s\n", algo)
			w.Write([]byte("Đã đổi thuật toán thành công: " + algo))
		} else {
			w.Write([]byte("Thuật toán không hợp lệ! Dùng: round-robin, weighted, load-based, maglev"))
		}
	})

	// API Routing chinh
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// 1. ĐỌC TRẠNG THÁI (Zero-lock, siêu tốc)
		state := pool.state.Load().(*RoutingState)
		currentAlgo := state.Algo

		// 2. LẤY HASH KEY (Chỉ đọc Body nếu đang dùng Maglev)
		var hashKey []byte
		if currentAlgo == "maglev" {
			// (Tuỳ chọn: Nếu bạn đã xài gjson thì gọi gjson.GetBytes(bodyBytes, "supi").String() ở đây)
			var err error
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				hashKey = bodyBytes // Hoặc parse SUPI nếu cần chính xác
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		}

		// 3. TÌM PEER
		peer := pool.GetNextPeer(hashKey)
		if peer != nil {
			atomic.AddInt32(&peer.ActiveRequests, 1)
			peer.ReverseProxy.ServeHTTP(w, r)
			atomic.AddInt32(&peer.ActiveRequests, -1)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "ERROR", "cause": "NO_BACKEND_AVAILABLE"}`))
	})

	// Khoi tao HTTP/2 Cleartext Server (theo chuan 3GPP)
	server := &http.Server{
		Addr:         ":8000",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Ho tro HTTP/2 khong ma hoa (h2c) truc tiep tu thu vien chuan
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s\n", port)
	log.Fatal(server.ListenAndServe())
}
