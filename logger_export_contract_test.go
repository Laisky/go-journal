package journal_test

import (
	"reflect"
	"testing"

	journal "github.com/Laisky/go-journal"
	utils "github.com/Laisky/go-utils"
)

func TestRegressionLoggerExportedConcreteType(t *testing.T) {
	got := reflect.TypeOf(&journal.Logger).Elem()
	want := reflect.TypeOf((*utils.LoggerType)(nil))
	if got != want {
		t.Fatalf("exported Logger type changed: got %v, want %v; concrete callers lose the embedded zap.Logger", got, want)
	}
}
