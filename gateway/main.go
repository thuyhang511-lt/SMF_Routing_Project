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
	// Grab a free transport from the pool to serve this request.
	transport := tp.NextTransport()
	return transport.RoundTrip(req)
}

// NewTransportPool builds a pool of h2c transports (clients).
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

// routeSnapshot is an immutable view of the routing state. The request hot
// path loads it with a single atomic load (no lock); writers (heartbeat,
// health checker, algo switch) publish a brand-new snapshot under the pool
// mutex. This replaces the per-request RWMutex.RLock, which was the dominant
// gateway overhead when every request contended on the same lock.
type routeSnapshot struct {
	algo    string
	healthy []*algorithm.Backend
	maglev  *algorithm.Maglev // non-nil only when algo == "maglev"
}

type BackendPool struct {
	// Writer-side state, guarded by mutex. Never read on the hot path.
	mutex    sync.Mutex
	backends []*algorithm.Backend
	algo     string
	registry map[string]time.Time // last-seen time per PDU instance

	lbState *algorithm.LoadBalancerState // round-robin counter (atomic), stable pointer

	// Lock-free routing snapshot read by every request.
	snap atomic.Pointer[routeSnapshot]
}

// publish rebuilds the lock-free routing snapshot from the current backend
// set. The caller MUST hold p.mutex. Writers are rare (heartbeats every 3s,
// health check every 5s, manual algo switch), so the cost of rebuilding the
// healthy slice and the maglev table here is off the hot path.
func (p *BackendPool) publish() {
	healthy := make([]*algorithm.Backend, 0, len(p.backends))
	for _, b := range p.backends {
		if b.Alive {
			healthy = append(healthy, b)
		}
	}

	var mg *algorithm.Maglev
	if p.algo == "maglev" {
		mg = &algorithm.Maglev{}
		mg.Build(healthy)
	}

	p.snap.Store(&routeSnapshot{
		algo:    p.algo,
		healthy: healthy,
		maglev:  mg,
	})
}

// pick chooses a backend from an already-loaded snapshot. It performs no
// locking; the round-robin counter and load-based counters are atomic, and
// the weighted algorithm serializes itself via lbState.Mutex.
func (p *BackendPool) pick(s *routeSnapshot, bodyBytes []byte) *algorithm.Backend {
	if len(s.healthy) == 0 {
		return nil
	}

	var lb algorithm.LoadBalancer
	switch s.algo {
	case "weighted":
		lb = &algorithm.Weighted{Mu: &p.lbState.Mutex}
	case "load-based":
		lb = &algorithm.LoadBased{}
	case "maglev":
		lb = s.maglev
	default:
		lb = &algorithm.RoundRobin{State: p.lbState}
	}

	return lb.Next(s.healthy, bodyBytes)
}

type HeartbeatMsg struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type proxyBufferPool struct {
	pool sync.Pool
}

func (p *proxyBufferPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *proxyBufferPool) Put(b []byte) {
	p.pool.Put(b)
}

var sharedBufferPool = &proxyBufferPool{
	pool: sync.Pool{
		New: func() interface{} {
			return make([]byte, 32*1024) // ReverseProxy's default 32KB
		},
	},
}

func main() {

	// Initialize an empty BackendPool.
	pool := &BackendPool{
		backends: make([]*algorithm.Backend, 0),
		algo:     "round-robin",
		lbState:  &algorithm.LoadBalancerState{},
		registry: make(map[string]time.Time),
	}
	// Publish an initial (empty) snapshot so the hot path never sees nil.
	pool.publish()

	// Outbound (gateway -> backend) h2c transports. Fewer transports mean fewer
	// physical connections per backend, so more streams share one HTTP/2 write
	// loop and their frames coalesce into fewer socket writes (syscalls were
	// ~40% of the core in the profile). h2c also beat HTTP/1.1 here because H1
	// needs one connection per in-flight request and cannot coalesce.
	sharedTransport := NewTransportPool(1)
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

		changed := false
		exists := false
		for _, b := range pool.backends {
			if b.ID == msg.ID {
				if !b.Alive {
					fmt.Printf("[Gateway] PDU %s đã SỐNG LẠI tại %s\n", msg.ID, msg.URL)
					b.Alive = true
					changed = true
				}
				exists = true
				break
			}
		}

		if !exists {
			parsedUrl, _ := url.Parse(msg.URL)
			proxy := httputil.NewSingleHostReverseProxy(parsedUrl)
			proxy.Transport = sharedTransport
			proxy.BufferPool = sharedBufferPool

			newBackend := &algorithm.Backend{
				ID:           msg.ID,
				URL:          parsedUrl,
				ReverseProxy: proxy,
				Weight:       1,
				Alive:        true,
			}
			pool.backends = append(pool.backends, newBackend)
			fmt.Printf("[Gateway] Phát hiện PDU MỚI gia nhập vòng: %s (%s)\n", msg.ID, msg.URL)

			changed = true
		}

		if changed {
			pool.publish()
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

			if changed {
				pool.publish()
			}
			pool.mutex.Unlock()
		}
	}()

	// API to switch the active algorithm at runtime.
	mux.HandleFunc("/admin/algo", func(w http.ResponseWriter, r *http.Request) {
		algo := r.URL.Query().Get("name")
		if algo == "round-robin" || algo == "weighted" || algo == "load-based" || algo == "maglev" {
			pool.mutex.Lock()
			pool.algo = algo
			pool.publish()
			pool.mutex.Unlock()
			fmt.Printf(">> Đã đổi thuật toán sang: %s\n", algo)
			w.Write([]byte("Đã đổi thuật toán thành công:" + algo))
		} else {
			w.Write([]byte("Thuật toán không hợp lệ! Dùng: round-robin, weighted, load-based, maglev"))
		}
	})

	// Main routing entrypoint.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Lock-free load of the current routing snapshot.
		s := pool.snap.Load()

		// The body is only needed to extract the SUPI for maglev hashing.
		var bodyBytes []byte
		if s.algo == "maglev" {
			var err error
			bodyBytes, err = io.ReadAll(r.Body)
			if err == nil {
				// Restore the body so the reverse proxy can forward it.
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		}

		peer := pool.pick(s, bodyBytes)
		if peer != nil {
			if s.algo == "load-based" {
				// Track in-flight load only when the load-based algorithm
				// actually reads it; the atomic ops are pure overhead otherwise.
				atomic.AddInt32(&peer.ActiveRequests, 1)
				peer.ReverseProxy.ServeHTTP(w, r)
				atomic.AddInt32(&peer.ActiveRequests, -1)
			} else {
				peer.ReverseProxy.ServeHTTP(w, r)
			}
			return
		}
		// No live backend: return 503.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status": "ERROR", "cause": "NO_BACKEND_AVAILABLE"}`))
	})

	// HTTP/2 cleartext (h2c) server, per 3GPP SBI.
	//
	// Per-request Read/Write deadlines were the single biggest CPU sink under
	// load: each request re-arms a runtime timer, and with GOMAXPROCS=1 the
	// scheduler's runtime.(*timers).check burned ~20% of the core. Drop the
	// per-request deadlines and keep only a coarse IdleTimeout to reap dead
	// connections. (Acceptable here: this is an internal, trusted h2c hop.)
	server := &http.Server{
		Addr:        ":8000",
		Handler:     mux,
		IdleTimeout: 120 * time.Second,
	}

	// Enable unencrypted HTTP/2 (h2c) straight from the standard library.
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)

	port := ":8000"
	fmt.Printf("Gateway đang chạy tại cổng %s\n", port)
	log.Fatal(server.ListenAndServe())
}
