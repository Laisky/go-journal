package journal

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	utils "github.com/Laisky/go-utils"
)

func BenchmarkLock(b *testing.B) {
	b.Run("mutex", func(b *testing.B) {
		l := &sync.Mutex{}
		for i := 0; i < b.N; i++ {
			l.Lock()
			b.Log("yo")
			l.Unlock()
		}
	})

	b.Run("atomic", func(b *testing.B) {
		var (
			i uint64 = 0
		)
		for j := 0; j < b.N; j++ {
			atomic.CompareAndSwapUint64(&i, 0, 1)
			atomic.CompareAndSwapUint64(&i, 1, 0)
		}
	})
}

func fakedata(length int) map[int64]interface{} {
	m := make(map[int64]interface{}, length)
	for i := 0; i < length; i++ {
		m[int64(i)] = utils.RandomStringWithLength(100 + i)
	}

	return m
}

func TestJournal(t *testing.T) {
	var err error
	if err = Logger.ChangeLevel("error"); err != nil {
		t.Fatalf("set level: %+v", err)
	}
	dir, err := ioutil.TempDir("", "journal-test")
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("create directory: %v", dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := NewJournal(
		WithBufDirPath(dir),
		WithBufSizeByte(100),
		WithCommitIDTTL(1*time.Second),
	)
	if err != nil {
		t.Fatalf("%+v", err)
	}

	if err := j.Start(ctx); err != nil {
		t.Fatalf("%+v", err)
	}

	data := &Data{}
	threshold := int64(50)

	defer func() {
		j.Close()
		os.RemoveAll(dir)
	}()

	for id, val := range fakedata(1000) {
		data.Data = map[string]interface{}{"val": val}
		data.ID = id
		if err = j.WriteData(data); err != nil {
			t.Fatalf("got error: %+v", err)
		}

		if id < threshold { // not committed
			continue
		}

		if err = j.WriteId(id); err != nil {
			t.Fatalf("got error: %+v", err)
		}
	}

	// because journal will keep at least one journal, so need rotate twice
	if err = j.Rotate(ctx); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = j.Rotate(ctx); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	if !j.LockLegacy() {
		t.Fatal("can not lock legacy")
	}
	time.Sleep(1500 * time.Millisecond)
	i := 0
	for {
		if err = j.LoadLegacyBuf(data); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("got error: %+v", err)
		}

		t.Logf("got: %v", data.ID)
		if data.ID >= threshold {
			t.Errorf("should not got id: %+v", data.ID)
		}

		i++
	}

	if i != int(threshold) {
		t.Fatalf("expect %v, got %v", threshold, i)
	}
}

// Each calibration owns its journal and replay lease. Store measures data+ACK
// pairs; load measures a sealed replay followed by its acknowledgement.
func BenchmarkJournal(b *testing.B) {
	newJournal := func(b *testing.B) *Journal {
		b.Helper()
		j, err := NewJournal(WithBufDirPath(b.TempDir()), WithBufSizeByte(1<<20),
			WithIsAggresiveGC(false), WithFlushInterval(time.Hour),
			WithRotateDuration(time.Hour), WithRotateCheckInterval(time.Hour),
			WithCommitIDTTL(time.Hour))
		if err != nil {
			b.Fatal(err)
		}
		if err := j.Start(context.Background()); err != nil {
			b.Fatal(err)
		}
		b.Cleanup(j.Close)
		return j
	}
	b.Run("store", func(b *testing.B) {
		j := newJournal(b)
		data := &Data{Data: map[string]interface{}{"data": "xxx"}}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data.ID = int64(i + 1)
			if err := j.WriteData(data); err != nil {
				b.Fatal(err)
			}
			if err := j.WriteId(data.ID); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := j.Sync(); err != nil {
			b.Fatal(err)
		}
		j.Close()
		entries, err := os.ReadDir(j.bufDirPath)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		acks := NewInt64SetWithTTL(context.Background(), time.Hour)
		defer acks.Close()
		for _, entry := range entries {
			if strings.HasSuffix(entry.Name(), ".ids") {
				fp, err := os.Open(filepath.Join(j.bufDirPath, entry.Name()))
				if err != nil {
					b.Fatal(err)
				}
				dec, err := NewIdsDecoder(fp, false)
				if err != nil {
					fp.Close()
					b.Fatal(err)
				}
				err = dec.ReadAllToInt64Set(acks)
				fp.Close()
				if err != nil {
					b.Fatal(err)
				}
				continue
			}
			if !strings.HasSuffix(entry.Name(), ".buf") {
				continue
			}
			fp, err := os.Open(filepath.Join(j.bufDirPath, entry.Name()))
			if err != nil {
				b.Fatal(err)
			}
			dec, err := NewDataDecoder(fp, false)
			if err != nil {
				fp.Close()
				b.Fatal(err)
			}
			for {
				var got Data
				err := dec.Read(&got)
				if err == io.EOF {
					break
				}
				if err != nil {
					fp.Close()
					b.Fatal(err)
				}
				count++
				if got.ID != int64(count) || got.Data["data"] != "xxx" {
					fp.Close()
					b.Fatalf("changed benchmark record: %+v", got)
				}
			}
			if err := fp.Close(); err != nil {
				b.Fatal(err)
			}
		}
		if count != b.N || acks.GetLen() != b.N {
			b.Fatalf("persisted records=%d ACKs=%d want=%d", count, acks.GetLen(), b.N)
		}
		for i := 0; i < b.N; i++ {
			if !acks.CheckAndRemove(int64(i + 1)) {
				b.Fatalf("missing ACK %d", i+1)
			}
		}
		b.ReportMetric(float64(count)/float64(b.N), "records/op")
	})
	b.Run("load", func(b *testing.B) {
		j := newJournal(b)
		data := &Data{Data: map[string]interface{}{"data": "xxx"}}
		for i := 0; i < b.N; i++ {
			data.ID = int64(i + 1)
			if err := j.WriteData(data); err != nil {
				b.Fatal(err)
			}
		}
		if err := j.Rotate(context.Background()); err != nil {
			b.Fatal(err)
		}
		if !j.LockLegacy() {
			b.Fatal("cannot acquire replay lease")
		}
		b.Cleanup(func() { j.UnLockLegacy() })
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := j.LoadLegacyBuf(data); err != nil {
				b.Fatal(err)
			}
			if data.ID != int64(i+1) || data.Data["data"] != "xxx" {
				b.Fatalf("changed replay record: %+v", data)
			}
			if err := j.WriteId(data.ID); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := j.Sync(); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(1, "records/op")
	})
}
