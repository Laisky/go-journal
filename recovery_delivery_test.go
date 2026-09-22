package journal

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// These tests use the public journal contract. The caller never manufactures a
// loader file list and never rotates an extra time to make recovery work.
func TestRecoveryRetainedIDsAndNewestSegment(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			dir := t.TempDir()
			open := func() *Journal {
				t.Helper()
				j, err := NewJournal(WithBufDirPath(dir), WithBufSizeByte(1024*1024),
					WithIsCompress(compressed), WithRotateDuration(time.Hour), WithCommitIDTTL(time.Second))
				if err != nil { t.Fatal(err) }
				if err = j.Start(context.Background()); err != nil { t.Fatal(err) }
				return j
			}
			j := open()
			for _, id := range []int64{91, 7, 53} {
				if err := j.WriteData(&Data{ID: id, Data: map[string]interface{}{"payload": fmt.Sprint(id)}}); err != nil { t.Fatal(err) }
			}
			if err := j.WriteId(7); err != nil { t.Fatal(err) }
			if err := j.Sync(); err != nil { t.Fatal(err) }
			j.Close()
			j = open()
			defer j.Close()
			high, err := j.LoadMaxId()
			if err != nil { t.Fatal(err) }
			if high != 91 { t.Errorf("recovered high-water=%d; must include unacknowledged records (want 91)", high) }
			if !j.LockLegacy() { t.Fatal("cannot start recovery") }
			got := map[int64]string{}
			for {
				d := &Data{}
				err := j.LoadLegacyBuf(d)
				if err == io.EOF { break }
				if err != nil { t.Fatal(err) }
				got[d.ID] = d.Data["payload"].(string)
				// Recovery transfers every still-required record before EOF cleanup.
				if err := j.WriteData(d); err != nil { t.Fatal(err) }
			}
			if len(got) != 2 || got[91] != "91" || got[53] != "53" {
				t.Errorf("first recovery omitted newest sealed segment or replayed ACKed records: %v", got)
			}
		})
	}
}

func TestRecoveryDoesNotReadActiveSegment(t *testing.T) {
	j, err := NewJournal(WithBufDirPath(t.TempDir()), WithBufSizeByte(1024*1024), WithRotateDuration(time.Hour))
	if err != nil { t.Fatal(err) }
	if err = j.Start(context.Background()); err != nil { t.Fatal(err) }
	defer j.Close()
	if err = j.WriteData(&Data{ID: 1, Data: map[string]interface{}{"value": "live"}}); err != nil { t.Fatal(err) }
	if !j.LockLegacy() { t.Fatal("cannot acquire lock") }
	if err = j.LoadLegacyBuf(&Data{}); err != io.EOF { t.Fatalf("active writer must not be replayed: %v", err) }
}
