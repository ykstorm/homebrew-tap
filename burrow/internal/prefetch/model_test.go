package prefetch

import (
	"reflect"
	"testing"
)

func TestPredictRanksByCoAccess(t *testing.T) {
	m := NewModel()
	// Session 1 and 2: A with B. Session 3: A with C.
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s2", "A")
	m.Observe("s2", "B")
	m.Observe("s3", "A")
	m.Observe("s3", "C")

	got := m.Predict("A", 2)
	if !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatalf("predict=%v want [B C]", got)
	}
}

func TestPredictExcludesSeedAndUnknown(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	if got := m.Predict("A", 5); !reflect.DeepEqual(got, []string{"B"}) {
		t.Fatalf("predict=%v want [B]", got)
	}
	if got := m.Predict("Z", 5); len(got) != 0 {
		t.Fatalf("predict unknown=%v want empty", got)
	}
}

func TestObserveIsIdempotentWithinSession(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s1", "B") // repeat must not double-count
	m.Observe("s2", "A")
	m.Observe("s2", "C")
	// A-B counted once, A-C counted once => tie, broken by name asc.
	if got := m.Predict("A", 2); !reflect.DeepEqual(got, []string{"B", "C"}) {
		t.Fatalf("predict=%v want [B C]", got)
	}
}
