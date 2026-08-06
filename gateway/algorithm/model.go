package algorithm

import (
	"net/http/httputil"
	"net/url"
	"sync"
)

type Backend struct {
	URL          *url.URL
	Alive        bool
	ReverseProxy *httputil.ReverseProxy

	// Thuat toan Weighted Round Robin
	Weight        int
	CurrentWeight int

	// Thuat toan Load-based
	ActiveRequests int32

	ID string
}

type LoadBalancer interface {
	Next(healthyBackends []*Backend, bodyBytes []byte) *Backend
}

type LoadBalancerState struct {
	Counter uint64 // Round Robin
	Mutex   sync.Mutex
}
