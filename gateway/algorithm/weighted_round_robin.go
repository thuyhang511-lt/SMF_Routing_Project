package algorithm

type Weighted struct{}

func (w *Weighted) Next(healthy []*Backend, bodyBytes []byte) *Backend {
	if len(healthy) == 0 {
		return nil
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
