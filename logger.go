package journal

import (
	"github.com/Laisky/zap"
	"github.com/Laisky/zap/zapcore"
	"reflect"
)

// Logger is the logging contract consumed by journals and replay readers.
// It deliberately requires no factory, child, level, flush or Zap-core API.
type Logger interface {
	Debug(string, ...zapcore.Field)
	Info(string, ...zapcore.Field)
	Warn(string, ...zapcore.Field)
	Error(string, ...zapcore.Field)
	Panic(string, ...zapcore.Field)
}

var defaultLoggerLevel = zap.NewAtomicLevelAt(zapcore.InfoLevel)
var defaultLogger = newDefaultLogger()

func newDefaultLogger() *zap.Logger {
	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.Level = defaultLoggerLevel
	logger, err := config.Build()
	if err != nil {
		panic("journal: initialize default logger: " + err.Error())
	}
	return logger.Named("go-journal")
}

// Check nil implementations only; no logger is converted or unwrapped.
func loggerIsNil(logger Logger) bool {
	if logger == nil {
		return true
	}
	value := reflect.ValueOf(logger)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
