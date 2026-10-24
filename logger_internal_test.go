package journal

import "github.com/Laisky/zap/zapcore"

func setTestLogLevel(text string) error {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(text)); err != nil {
		return err
	}
	defaultLoggerLevel.SetLevel(level)
	return nil
}
