package journal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func regressionReadOnlyFile(t *testing.T) *os.File {
	t.Helper()
	name := filepath.Join(t.TempDir(), "readonly")
	if err := os.WriteFile(name, nil, 0600); err != nil {
		t.Fatal(err)
	}
	fp, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fp.Close() })
	return fp
}

func TestRegressionDataWritePropagatesFlushError(t *testing.T) {
	enc, err := NewDataEncoder(regressionReadOnlyFile(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(&Data{ID: 42, Data: map[string]interface{}{"message": "hello"}}); err == nil {
		t.Error("a write to a read-only file was reported as successful")
	}
}

func TestRegressionIDWritePropagatesFlushError(t *testing.T) {
	enc, err := NewIdsEncoder(regressionReadOnlyFile(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Write(42); err == nil {
		t.Error("an ID write to a read-only file was reported as successful")
	}
}

func TestRegressionFailedIDWriteIsNotCommitted(t *testing.T) {
	enc, err := NewIdsEncoder(regressionReadOnlyFile(t), false)
	if err != nil {
		t.Fatal(err)
	}
	ids := NewInt64Set()
	j := &Journal{idsEnc: enc, legacy: &LegacyLoader{ids: ids}}
	if err := j.WriteId(42); err == nil {
		t.Error("failed acknowledgement write returned no error")
	}
	if ids.CheckAndRemove(42) {
		t.Error("failed acknowledgement was published in the committed-ID index")
	}
}

func TestRegressionJournalPropagatesDataFlushError(t *testing.T) {
	for _, closeEncoder := range []bool{false, true} {
		enc, err := NewDataEncoder(regressionReadOnlyFile(t), false)
		if err != nil {
			t.Fatal(err)
		}
		if err := enc.writer.WriteInt64(42); err != nil {
			t.Fatal(err)
		}
		j := &Journal{option: newOption(), dataEnc: enc}
		if closeEncoder {
			err = j.flushAndClose()
		} else {
			err = j.Flush()
		}
		if err == nil {
			t.Errorf("data flush failure lost (close=%v)", closeEncoder)
		}
	}
}

func regressionWriteIDs(t *testing.T, name string, ids ...int64) {
	t.Helper()
	fp, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()
	enc, err := NewIdsEncoder(fp, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := enc.Write(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionLegacyMaximumAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	first, last := filepath.Join(dir, "first.ids"), filepath.Join(dir, "last.ids")
	regressionWriteIDs(t, first, 3, 99, 4)
	regressionWriteIDs(t, last, 2, 7)
	loader := &LegacyLoader{logger: Logger, idsFNames: []string{first, last}}
	got, err := loader.LoadMaxId()
	if err != nil {
		t.Fatal(err)
	}
	if got != 99 {
		t.Errorf("maximum=%d, want 99 (not the maximum from only the last file)", got)
	}
}

func TestRegressionInt64SetCountsUniqueIDs(t *testing.T) {
	s := NewInt64Set()
	for i := 0; i < 10; i++ {
		s.AddInt64(42)
	}
	if got := s.GetLen(); got != 1 {
		t.Errorf("set length=%d after duplicate inserts, want 1", got)
	}
	if !s.CheckAndRemove(42) {
		t.Error("inserted ID missing")
	}
	if s.CheckAndRemove(42) {
		t.Error("removed ID still present")
	}
	if got := s.GetLen(); got != 0 {
		t.Errorf("set length=%d after removal, want 0", got)
	}
}

func TestRegressionTTLExpiredCountConcurrent(t *testing.T) {
	for round := 0; round < 100; round++ {
		s := &Int64SetWithTTL{ng: &sync.Map{}, og: &sync.Map{}, ogN: 1}
		s.og.Store(int64(42), time.Now().Unix()-60)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; s.CheckAndRemove(42) }()
		}
		close(start)
		wg.Wait()
		if got := s.GetLen(); got != 0 {
			t.Fatalf("expired-ID count=%d, want 0", got)
		}
	}
}

func TestRegressionTTLRetainsAcknowledgementForMultipleCopies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := NewInt64SetWithTTL(ctx, time.Hour)
	s.AddInt64(42)
	for i := 0; i < 3; i++ {
		if !s.CheckAndRemove(42) {
			t.Fatal("membership lookup consumed an acknowledgement needed by another replay copy")
		}
	}
	if got := s.GetLen(); got != 1 {
		t.Errorf("length=%d, want 1", got)
	}
}

func TestRegressionTTLCloseAfterWorkerStopped(t *testing.T) {
	// Model the state after context cancellation: there is no receiving worker.
	s := &Int64SetWithTTL{stopChan: make(chan struct{})}
	done := make(chan struct{})
	go func() { s.Close(); s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Error("Close blocks without a worker or on repeated calls")
	}
}

func TestRegressionJournalCloseWithoutWorkers(t *testing.T) {
	j, err := NewJournal()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { j.Close(); j.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Error("journal Close blocks before Start or after workers stop")
	}
}

func TestRegressionConcurrentIDEncoder(t *testing.T) {
	fp, err := os.Create(filepath.Join(t.TempDir(), "concurrent.ids"))
	if err != nil {
		t.Fatal(err)
	}
	defer fp.Close()
	enc, err := NewIdsEncoder(fp, false)
	if err != nil {
		t.Fatal(err)
	}
	const n = 256
	start := make(chan struct{})
	var wg sync.WaitGroup
	for id := int64(1); id <= n; id++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			if err := enc.Write(id); err != nil {
				t.Errorf("write ID: %v", err)
			}
		}(id)
	}
	close(start)
	wg.Wait()
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fp.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	dec, err := NewIdsDecoder(fp, false)
	if err != nil {
		t.Fatal(err)
	}
	ids := NewInt64Set()
	if err := dec.ReadAllToInt64Set(ids); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= n; id++ {
		if !ids.CheckAndRemove(id) {
			t.Errorf("ID %d lost/corrupted", id)
		}
	}
}

func TestRegressionLegacyCleanupRequiresSuccessfulFlush(t *testing.T) {
	dir := t.TempDir()
	old, current := filepath.Join(dir, "old.data"), filepath.Join(dir, "current.data")
	for _, name := range []string{old, current} {
		if err := os.WriteFile(name, []byte("retained"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	j, err := NewJournal()
	if err != nil {
		t.Fatal(err)
	}
	j.legacy = &LegacyLoader{logger: Logger, ids: NewInt64Set(), dataFNames: []string{old, current}, dataFilesLen: 1, dataFileIdx: 0}
	j.dataEnc, err = NewDataEncoder(regressionReadOnlyFile(t), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.dataEnc.writer.WriteInt64(42); err != nil {
		t.Fatal(err)
	}
	if !j.LockLegacy() {
		t.Fatal("cannot acquire legacy lock")
	}
	err = j.LoadLegacyBuf(&Data{})
	if err == nil || err == io.EOF {
		t.Errorf("failed durability barrier must be returned, got %v", err)
	}
	if _, err := os.Stat(old); err != nil {
		t.Errorf("old durable copy was deleted before replacement flush succeeded: %v", err)
	}
}
