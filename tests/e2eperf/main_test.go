//go:build linux

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	journal "github.com/Laisky/go-journal"
)

func TestDownstreamReceiptIsRequired(t *testing.T) {
	for _, body := range []string{`{}`, `{"id":1,"hash":"bad","durable":true}`, `{"id":1,"hash":"bad","durable":false}`, `not-json`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); w.Write([]byte(body)) }))
		err := deliver(server.Client(), options{Sink: server.URL, Mode: "seed"}, 1, "body")
		server.Close()
		if err == nil {
			t.Fatalf("accepted bad downstream receipt %q", body)
		}
	}
}
func TestEmptyRecoveryAndIndependentValues(t *testing.T) {
	o := options{Count: 4, Payload: 64, Writers: 2, AckPercent: 0, Mode: "seed"}
	j, err := journal.NewJournal(journal.WithBufDirPath(t.TempDir()), journal.WithIsAggresiveGC(false))
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	r := new(result)
	if err = seed(j, o, r, http.DefaultClient); err != nil {
		t.Fatal(err)
	}
	if err = j.Rotate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !j.LockLegacy() {
		t.Fatal("lease")
	}
	defer j.UnLockLegacy()
	for i := 0; i < 4; i++ {
		d := new(journal.Data)
		if err = j.LoadLegacyBuf(d); err != nil {
			t.Fatal(err)
		}
		if err = validate(d, 64); err != nil {
			t.Fatal(err)
		}
		if err = j.WriteId(d.ID); err != nil {
			t.Fatal(err)
		}
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err = j.LoadLegacyBuf(new(journal.Data)); err != io.EOF {
		t.Fatalf("EOF: %v", err)
	}
}
