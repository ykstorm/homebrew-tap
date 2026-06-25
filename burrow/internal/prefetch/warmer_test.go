package prefetch

import (
	"errors"
	"reflect"
	"testing"
)

type fakePrefetcher struct {
	warmed []string
	fail   map[string]bool
}

func (f *fakePrefetcher) Prefetch(tag string) error {
	if f.fail[tag] {
		return errors.New("boom")
	}
	f.warmed = append(f.warmed, tag)
	return nil
}

func TestWarmerWarmsPredictions(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s2", "A")
	m.Observe("s2", "B")
	m.Observe("s3", "A")
	m.Observe("s3", "C")

	fp := &fakePrefetcher{}
	w := NewWarmer(fp, m, 2)
	warmed := w.WarmFor("A")
	if !reflect.DeepEqual(warmed, []string{"B", "C"}) {
		t.Fatalf("warmed=%v want [B C]", warmed)
	}
	if !reflect.DeepEqual(fp.warmed, []string{"B", "C"}) {
		t.Fatalf("prefetcher saw %v", fp.warmed)
	}
}

func TestWarmerIsBestEffort(t *testing.T) {
	m := NewModel()
	m.Observe("s1", "A")
	m.Observe("s1", "B")
	m.Observe("s1", "C")

	fp := &fakePrefetcher{fail: map[string]bool{"B": true}}
	w := NewWarmer(fp, m, 5)
	warmed := w.WarmFor("A") // B fails, C still warms
	for _, tag := range warmed {
		if tag == "B" {
			t.Fatal("failed prefetch must not be reported warmed")
		}
	}
	found := false
	for _, tag := range warmed {
		if tag == "C" {
			found = true
		}
	}
	if !found {
		t.Fatal("a failure must not stop warming the rest")
	}
}
