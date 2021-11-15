package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gutils "github.com/Laisky/go-utils"
	"github.com/cockroachdb/pebble"
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
		m[int64(i)] = gutils.RandomStringWithLength(100 + i)
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

		if err = j.WriteID(id); err != nil {
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

/*
BenchmarkJournal/store-16                 233775              5310 ns/op              80 B/op          5 allocs/op
BenchmarkJournal/pebble-16                123537              9562 ns/op             319 B/op          0 allocs/op
*/
func BenchmarkJournal(b *testing.B) {
	dir, err := ioutil.TempDir("", "journal-test-bench")
	if err != nil {
		log.Fatal(err)
	}
	b.Logf("create directory: %v", dir)
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := NewJournal(
		WithBufDirPath(dir),
		WithBufSizeByte(100),
	)
	if err != nil {
		b.Fatalf("%+v", err)
	}

	if err := j.Start(ctx); err != nil {
		b.Fatalf("%+v", err)
	}

	data := &Data{
		Data: map[string]interface{}{"data": "xxx"},
		ID:   1,
	}
	id := int64(1)

	b.Run("store", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err = j.WriteData(data); err != nil {
				b.Fatalf("got error: %+v", err)
			}

			if err = j.WriteID(id); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	dataB, err := gutils.JSON.Marshal(data)
	gutils.PanicIfErr(err)

	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, data.ID)
	gutils.PanicIfErr(err)
	idB := buf.Bytes()

	// peddle
	pdb, err := pebble.Open(filepath.Join(dir, "peddle"), &pebble.Options{})
	gutils.PanicIfErr(err)
	b.Run("pebble", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := pdb.Set(idB, dataB, pebble.NoSync)
			gutils.PanicIfErr(err)
		}
	})

	if err = j.Rotate(ctx); err != nil {
		b.Fatalf("got error: %+v", err)
	}

	b.Run("load", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			j.LockLegacy()
			defer j.UnLockLegacy()
			if err = j.LoadLegacyBuf(data); err == io.EOF {
				return
			} else if err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})
}
