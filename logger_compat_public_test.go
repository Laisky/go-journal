package journal_test

import (
	"context"
	"sync"
	"testing"

	journal "github.com/Laisky/go-journal/v2"
	"github.com/Laisky/zap"
	"github.com/Laisky/zap/zapcore"
)

func TestRegressionLoggerInterfaceWorkflow(t *testing.T) {
	var entries []zapcore.Entry
	var entriesMu sync.Mutex
	logger, err := zap.NewProduction(
		zap.Hooks(func(entry zapcore.Entry) error {
			entriesMu.Lock()
			entries = append(entries, entry)
			entriesMu.Unlock()
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	logger = logger.Named("journal-consumer")
	j, err := journal.NewJournal(journal.WithLogger(logger),
		journal.WithBufDirPath(t.TempDir()), journal.WithIsAggresiveGC(false))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := j.WriteData(&journal.Data{ID: 42, Data: map[string]interface{}{"payload": "retained"}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Flush(); err != nil {
		t.Fatal(err)
	}
	entriesMu.Lock()
	defer entriesMu.Unlock()
	found := false
	for _, entry := range entries {
		if entry.Message == "new journal" && entry.LoggerName == "journal-consumer" {
			found = true
		}
	}
	if !found {
		t.Fatal("journal creation did not use the caller's logger")
	}
}

func TestRegressionLoggerRejectsNil(t *testing.T) {
	for _, logger := range []journal.Logger{nil, (*zap.Logger)(nil), (*callerLogger)(nil)} {
		if _, err := journal.NewJournal(journal.WithLogger(logger)); err == nil {
			t.Fatal("nil logger was accepted")
		}
	}
}
