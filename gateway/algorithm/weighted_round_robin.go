package algorithm

import "sync"

// Weighted implements smooth Weighted Round Robin. Next mutates the shared
// Backend.CurrentWeight fields, so concurrent callers must be serialized;
// Mu (supplied by the pool) guards that read-modify-write. Without it,
// multiple in-flight requests race on CurrentWeight (go build -race flags it)
// and the weight distribution is corrupted.
type Weighted struct {
	Mu *sync.Mutex
}

func (w *Weighted) Next(healthy []*Backend, bodyBytes []byte) *Backend {
	if len(healthy) == 0 {
		return nil
	}

	if w.Mu != nil {
		w.Mu.Lock()
		defer w.Mu.Unlock()
	}

	var best *Backend
	totalWeight := 0

	for _, b := range healthy {
		b.CurrentWeight += b.Weight
		totalWeight += b.Weight

		if best == nil || b.CurrentWeight > best.CurrentWeight {
			best = b
		}
	}

	if best != nil {
		best.CurrentWeight -= totalWeight
	}
	return best
}
