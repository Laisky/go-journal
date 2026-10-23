package journal_test

import (
	journal "github.com/Laisky/go-journal/v2"
	"github.com/Laisky/zap"
	"testing"
)

func TestRegressionLoggerExportedConcreteType(t *testing.T) {
	// Producers retain their concrete API; the journal needs only its own interface.
	var child *zap.Logger = zap.NewNop().Named("child").With(zap.String("field", "kept")).WithOptions()
	var sink journal.Logger = child
	if sink == nil {
		t.Fatal("typed logger cannot be injected")
	}
}
