package algorithm

import "sync/atomic"

type RoundRobin struct {
	State *LoadBalancerState
}

func (rr *RoundRobin) Next(healthy []*Backend, bodyBytes []byte) *Backend {
	if len(healthy) == 0 {
		return nil
	}
	next := atomic.AddUint64(&rr.State.Counter, 1)
	idx := int((next - 1) % uint64(len(healthy)))
	return healthy[idx]
}
