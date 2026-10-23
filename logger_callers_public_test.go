package journal_test

import (
	"context"
	journal "github.com/Laisky/go-journal/v2"
	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/Laisky/zap/zapcore"
	"sync"
	"testing"
	"time"
)

var _ func(journal.Logger) journal.OptionFunc = journal.WithLogger
var _ func(context.Context, journal.Logger, []string, []string, bool, time.Duration) *journal.LegacyLoader = journal.NewLegacyLoader

// The consumer has no Clone/Named/WithOptions, level or embedded Zap methods.
type callerLogger struct {
	mu       sync.Mutex
	messages []string
}

func (l *callerLogger) record(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, s)
}
func (l *callerLogger) Debug(s string, _ ...zapcore.Field) { l.record(s) }
func (l *callerLogger) Info(s string, _ ...zapcore.Field)  { l.record(s) }
func (l *callerLogger) Warn(s string, _ ...zapcore.Field)  { l.record(s) }
func (l *callerLogger) Error(s string, _ ...zapcore.Field) { l.record(s) }
func (l *callerLogger) Panic(s string, _ ...zapcore.Field) { panic(s) }

var _ journal.Logger = (*callerLogger)(nil)
var _ journal.Logger = (*zap.Logger)(nil)

func TestLoggerConcreteAndInterfaceCallers(t *testing.T) {
	factory, err := utils.NewConsoleLoggerWithName("sdk-caller", "error")
	if err != nil {
		t.Fatal(err)
	}
	custom := &callerLogger{}
	for _, logger := range []journal.Logger{factory.Named("journal"), zap.NewNop().Named("plain-zap"), custom} {
		j, err := journal.NewJournal(journal.WithLogger(logger), journal.WithBufDirPath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		if err = j.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err = j.WriteData(&journal.Data{ID: 42, Data: map[string]interface{}{"payload": "logger contract"}}); err != nil {
			t.Fatal(err)
		}
		if err = j.Sync(); err != nil {
			t.Fatal(err)
		}
		j.Close()
		ctx, cancel := context.WithCancel(t.Context())
		loader := journal.NewLegacyLoader(ctx, logger, nil, nil, false, time.Minute)
		loader.AddID(42)
		if !loader.CheckAndRemove(42) || !loader.CheckAndRemove(42) || loader.GetIdsLen() != 1 {
			t.Fatal("logger changed retained ACK membership")
		}
		cancel()
	}
	custom.mu.Lock()
	defer custom.mu.Unlock()
	found := false
	for _, s := range custom.messages {
		if s == "new journal" {
			found = true
		}
	}
	if !found {
		t.Fatal("custom small logger was not used")
	}
}
