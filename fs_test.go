package journal

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/ncw/directio"
)

const (
	testBufFileSizeBytes = 1000000
)

type testFNameCase struct {
	OldFName, ExpectFName, NowTS string
}

func TestGenerateNewBufFName(t *testing.T) {
	var (
		err      error
		now      time.Time
		newFName string
		cases    = []*testFNameCase{
			{
				OldFName:    "20060102_00000001.buf",
				ExpectFName: "20060102_00000002.buf",
				NowTS:       "20060102-0700",
			},
			{
				OldFName:    "20060102_00000009.buf",
				ExpectFName: "20060102_00000010.buf",
				NowTS:       "20060102-0700",
			},
			{
				OldFName:    "20060102_00000001.ids",
				ExpectFName: "20060102_00000002.ids",
				NowTS:       "20060102-0700",
			},
			{
				OldFName:    "20060102_00000002.buf",
				ExpectFName: "20060104_00000001.buf",
				NowTS:       "20060104-0700",
			},
			{
				OldFName:    "20060102_00000002.buf",
				ExpectFName: "20060103_00000001.buf",
				NowTS:       "20060103-0600",
			},
		}
	)

	for _, testcase := range cases {
		now, err = time.Parse("20060102-0700", testcase.NowTS)
		if err != nil {
			t.Fatalf("got error: %+v", err)
		}
		newFName, err = GenerateNewBufFName(now, testcase.OldFName, false)
		if err != nil {
			t.Fatalf("got error: %+v", err)
		}
		if newFName != testcase.ExpectFName {
			t.Errorf("expect %v, got %v", testcase.ExpectFName, newFName)
		}
	}
}

func TestPrepareNewBufFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "journal-test-fs")
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("create directory: %v", dir)
	defer os.RemoveAll(dir)

	bufStat, err := PrepareNewBufFile(dir, nil, true, false, testBufFileSizeBytes)
	if err != nil {
		t.Fatalf("got error: %+v", err)
	}
	defer bufStat.NewDataFp.Close()
	defer bufStat.NewIDsFp.Close()

	_, err = bufStat.NewDataFp.WriteString("test data")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	_, err = bufStat.NewIDsFp.WriteString("test ids")
	if err != nil {
		t.Fatalf("%+v", err)
	}

	err = bufStat.NewDataFp.Sync()
	if err != nil {
		t.Fatalf("%+v", err)
	}
	err = bufStat.NewIDsFp.Sync()
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

const (
	benchmarkFsDir = "/data/fluentd/go-utils/"
	// benchmarkFsDir = "/Users/laisky/Downloads/"
)

func BenchmarkFSPreallocate(b *testing.B) {
	var err error
	// logger.ChangeLevel("error")
	if err = logger.ChangeLevel("error"); err != nil {
		b.Fatalf("set level: %+v", err)
	}
	// create data files
	dataFp1, err := directio.OpenFile(benchmarkFsDir+"fp1.dat", os.O_RDWR|os.O_CREATE, FileMode)
	// dataFp1, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	defer dataFp1.Close()
	defer os.Remove(dataFp1.Name())
	b.Logf("create file name: %v", dataFp1.Name())

	dataFp2, err := directio.OpenFile(benchmarkFsDir+"fp2.dat", os.O_RDWR|os.O_CREATE, FileMode)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	defer dataFp2.Close()
	defer os.Remove(dataFp2.Name())
	b.Logf("create file name: %v", dataFp2.Name())

	dataFp3, err := directio.OpenFile(benchmarkFsDir+"fp3.dat", os.O_RDWR|os.O_CREATE, FileMode)
	if err != nil {
		b.Fatalf("%+v", err)
	}
	defer dataFp3.Close()
	defer os.Remove(dataFp3.Name())
	b.Logf("create file name: %v", dataFp3.Name())

	payload := make([]byte, 1024)
	b.Run("normal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = dataFp1.Write(payload); err != nil {
				b.Fatalf("write: %+v", err)
			}
			// dataFp1.Sync()
		}
	})

	if err = fileutil.Preallocate(dataFp2, 1024*1024*1000, false); err != nil {
		b.Fatalf("prealloc: %+v", err)
	}
	b.ResetTimer()
	b.Run("preallocate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = dataFp2.Write(payload); err != nil {
				b.Fatalf("write: %+v", err)
			}
			// dataFp2.Sync()
		}
	})

	if err = fileutil.Preallocate(dataFp3, 1024*1024*1000, true); err != nil {
		b.Fatalf("prealloc: %+v", err)
	}
	b.ResetTimer()
	b.Run("preallocate with extended", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = dataFp3.Write(payload); err != nil {
				b.Fatalf("write: %+v", err)
			}
			// dataFp3.Sync()
		}
	})

}

func BenchmarkWrite(b *testing.B) {
	fp, err := ioutil.TempFile("", "fs-test")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	// fp, err := os.OpenFile("/data/go/src/github.com/Laisky/go-utils/journal/benchmark/test/test.data", os.O_RDWR|os.O_CREATE, 0664)
	// if err != nil {
	// 	b.Fatalf("got error: %+v", err)
	// }
	defer fp.Close()
	defer os.Remove(fp.Name())
	b.Logf("create file name: %v", fp.Name())

	data2K := []byte(utils.RandomStringWithLength(2048))
	b.Run("direct write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fp.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf := bufio.NewWriter(fp)
	b.Run("write default buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	b.Run("write default buf with flush", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf4KB := bufio.NewWriterSize(fp, 1024*4)
	b.Run("write 4KB buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf4KB.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf4KB.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf8KB := bufio.NewWriterSize(fp, 1024*8)
	b.Run("write 8KB buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf8KB.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf8KB.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf16KB := bufio.NewWriterSize(fp, 1024*16)
	b.Run("write 16KB buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf16KB.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf16KB.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf1M := bufio.NewWriterSize(fp, 1024*1024)
	b.Run("write 1M buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf1M.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf1M.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	fpBuf4M := bufio.NewWriterSize(fp, 1024*1024*4)
	b.Run("write 4M buf", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err = fpBuf4M.Write(data2K); err != nil {
				b.Fatalf("got error: %+v", err)
			}
			if err = fpBuf4M.Flush(); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

}

func BenchmarkData(b *testing.B) {
	dir, err := ioutil.TempDir("", "journal-test-bench-data")
	if err != nil {
		log.Fatal(err)
	}
	b.Logf("create directory: %v", dir)
	// var err error
	// dir := "/data/go/src/github.com/Laisky/go-utils/journal/benchmark/test"
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	j, err := NewJournal(
		WithBufDirPath(dir),
		WithBufSizeByte(314572800),
	)
	if err != nil {
		b.Fatalf("%+v", err)
	}

	if err := j.Start(ctx); err != nil {
		b.Fatalf("%+v", err)
	}

	data := &Data{
		ID:   1000,
		Data: map[string]interface{}{"data": utils.RandomStringWithLength(2048)},
	}
	b.Logf("write data: %+v", data)
	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err = j.WriteData(data); err != nil {
				b.Fatalf("got error: %+v", err)
			}
		}
	})

	if err = j.Flush(); err != nil {
		b.Fatalf("got error: %+v", err)
	}
	if err = j.Rotate(ctx); err != nil {
		b.Fatalf("got error: %+v", err)
	}
	b.Run("read", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data.ID = 0
			if err = j.LoadLegacyBuf(data); err == io.EOF {
				return
			} else if err != nil {
				b.Fatalf("got error: %+v", err)
			}

			if data.ID != 1000 {
				b.Fatal("read data error")
			}
		}
	})
}
