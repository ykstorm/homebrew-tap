package raftindex

import (
	"bytes"
	"io"
	"testing"
)

type nopCloser struct{ io.Reader }

func (nopCloser) Close() error { return nil }

func TestFSMApplySet(t *testing.T) {
	f := newFSM()
	cmd, _ := encodeSet("sbx@latest", "hash1")
	if resp := f.applyBytes(cmd); resp != nil {
		t.Fatalf("apply returned %v", resp)
	}
	if h, ok := f.get("sbx@latest"); !ok || h != "hash1" {
		t.Fatalf("got %q,%v", h, ok)
	}
}

func TestFSMSnapshotRestore(t *testing.T) {
	f := newFSM()
	cmd, _ := encodeSet("sbx@latest", "hash1")
	f.applyBytes(cmd)

	var buf bytes.Buffer
	if err := f.snapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	f2 := newFSM()
	if err := f2.Restore(nopCloser{bytes.NewReader(buf.Bytes())}); err != nil {
		t.Fatal(err)
	}
	if h, ok := f2.get("sbx@latest"); !ok || h != "hash1" {
		t.Fatalf("restored got %q,%v", h, ok)
	}
}
