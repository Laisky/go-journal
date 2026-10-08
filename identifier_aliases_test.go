package journal_test

import (
	"context"
	"errors"
	journal "github.com/Laisky/go-journal"
	"os"
	"testing"
)

func TestIdentifierAliasesPreserveLifecycleAndReplay(t *testing.T) {
	j := behaviorNew(t, t.TempDir(), false)
	for _, write := range []func(int64) error{j.WriteId, j.WriteID} {
		if err := write(1); !errors.Is(err, journal.ErrNotStarted) {
			t.Fatalf("unstarted ACK: %v", err)
		}
	}
	behaviorCheck(t, j.Start(context.Background()))
	behaviorCheck(t, j.WriteData(behaviorData(1)))
	behaviorCheck(t, j.WriteData(behaviorData(2)))
	behaviorCheck(t, j.WriteID(2))
	behaviorCheck(t, j.Rotate(context.Background()))
	for _, maxID := range []func() (int64, error){j.LoadMaxId, j.LoadMaxID} {
		got, err := maxID()
		if err != nil || got != 2 {
			t.Fatalf("sealed maximum=%d: %v", got, err)
		}
	}
	got := behaviorReplay(t, j)
	if len(got) != 1 || got[1] == nil {
		t.Fatalf("alias changed replay: %+v", got)
	}
	j.Close()
	for _, write := range []func(int64) error{j.WriteId, j.WriteID} {
		if err := write(3); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("closed ACK: %v", err)
		}
	}
	for _, maxID := range []func() (int64, error){j.LoadMaxId, j.LoadMaxID} {
		if _, err := maxID(); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("closed maximum: %v", err)
		}
	}
}
