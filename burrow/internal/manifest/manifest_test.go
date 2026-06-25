package manifest

import "testing"

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	m := &Manifest{Name: "sbx", Version: "0.33.0", Size: 42, Chunks: []string{"aa", "bb"}}
	data, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != m.Name || got.Version != m.Version || got.Size != m.Size {
		t.Fatalf("scalar fields differ: %+v", got)
	}
	if len(got.Chunks) != 2 || got.Chunks[0] != "aa" || got.Chunks[1] != "bb" {
		t.Fatalf("chunks differ: %+v", got.Chunks)
	}
}

func TestHashIsStableAndContentSensitive(t *testing.T) {
	m1 := &Manifest{Name: "sbx", Version: "0.33.0", Size: 1, Chunks: []string{"aa"}}
	m2 := &Manifest{Name: "sbx", Version: "0.33.0", Size: 1, Chunks: []string{"aa"}}
	m3 := &Manifest{Name: "sbx", Version: "0.34.0", Size: 1, Chunks: []string{"aa"}}
	h1, _ := m1.Hash()
	h2, _ := m2.Hash()
	h3, _ := m3.Hash()
	if h1 != h2 {
		t.Fatalf("equal manifests hashed differently: %s vs %s", h1, h2)
	}
	if h1 == h3 {
		t.Fatal("different manifests hashed the same")
	}
}
