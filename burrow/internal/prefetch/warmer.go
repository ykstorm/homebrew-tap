package prefetch

// Prefetcher warms the cache for a single tag. Implemented by *node.Node.
type Prefetcher interface {
	Prefetch(tag string) error
}

// Warmer turns model predictions into proactive cache fills. Best-effort: a
// failed prefetch is skipped, never propagated.
type Warmer struct {
	pf    Prefetcher
	model *Model
	k     int
}

// NewWarmer builds a Warmer that warms up to k predictions per seed.
func NewWarmer(pf Prefetcher, model *Model, k int) *Warmer {
	return &Warmer{pf: pf, model: model, k: k}
}

// WarmFor predicts likely-next tags for seed and prefetches each, returning the
// tags successfully warmed (in prediction order).
func (w *Warmer) WarmFor(seed string) []string {
	preds := w.model.Predict(seed, w.k)
	warmed := make([]string, 0, len(preds))
	for _, tag := range preds {
		if err := w.pf.Prefetch(tag); err == nil {
			warmed = append(warmed, tag)
		}
	}
	return warmed
}
