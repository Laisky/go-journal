package journal_test

import (
	"context"
	"testing"
	"time"

	journal "github.com/Laisky/go-journal"
	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
)

// Preserve exact types, including callers that store exported functions.
var _ *utils.LoggerType = journal.Logger
var _ func(*utils.LoggerType) journal.OptionFunc = journal.WithLogger
var _ func(context.Context, *utils.LoggerType, []string, []string, bool, time.Duration) *journal.LegacyLoader = journal.NewLegacyLoader

type callerLogger struct{ utils.LoggerItf }

func TestLoggerConcreteAndInterfaceCallers(t *testing.T) {
	// Access to the concrete embedded zap logger remains source-compatible.
	var embedded *zap.Logger = journal.Logger.Logger
	if embedded == nil {
		t.Fatal("missing embedded logger")
	}
	factory, err := utils.NewConsoleLoggerWithName("interface-caller", "error")
	if err != nil {
		t.Fatal(err)
	}
	custom := &callerLogger{factory}
	concrete := journal.Logger
	for _, option := range []journal.OptionFunc{journal.WithLogger(concrete), journal.WithLoggerInterface(factory), journal.WithLoggerInterface(custom)} {
		j, err := journal.NewJournal(option, journal.WithBufDirPath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		if err := j.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := j.WriteData(&journal.Data{ID: 42, Data: map[string]interface{}{"payload": "logger compatibility"}}); err != nil {
			t.Fatal(err)
		}
		if err := j.Sync(); err != nil {
			t.Fatal(err)
		}
		j.Close()
	}
	if _, err := journal.NewJournal(journal.WithLogger(nil)); err == nil {
		t.Fatal("nil concrete logger accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, loader := range []*journal.LegacyLoader{
		journal.NewLegacyLoader(ctx, concrete, nil, nil, false, time.Minute),
		journal.NewLegacyLoaderWithLoggerInterface(ctx, factory, nil, nil, false, time.Minute),
		journal.NewLegacyLoaderWithLoggerInterface(ctx, custom, nil, nil, true, time.Minute),
	} {
		loader.AddID(42)
		if !loader.CheckAndRemove(42) || !loader.CheckAndRemove(42) || loader.GetIdsLen() != 1 {
			t.Fatal("logger adaptation changed non-consuming acknowledgement membership")
		}
	}
}
