package algorithm

import "sync/atomic"

type LoadBased struct{}

func (lb *LoadBased) Next(healthy []*Backend, bodyBytes []byte) *Backend {
	if len(healthy) == 0 {
		return nil
	}

	var best *Backend
	for _, b := range healthy {
		if best == nil || atomic.LoadInt32(&b.ActiveRequests) < atomic.LoadInt32(&best.ActiveRequests) {
			best = b
		}
	}
	return best
}
