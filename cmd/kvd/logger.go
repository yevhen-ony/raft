package main

import (
	"log/slog"
	"os"
	"strings"
)

type LoggerConfig struct {
	Level string `yaml:"level"`
}

func (lc *LoggerConfig) GetLevel() slog.Level {
	switch strings.ToLower(lc.Level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

func SetupLogger(cfg *LoggerConfig) *slog.Logger {
	handlerOpts := &slog.HandlerOptions{
		Level: cfg.GetLevel(),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				t := a.Value.Time()
				a.Value = slog.StringValue(t.Format("2006-01-02|15:04:05.00")) // <-- your format
			}
			return a
		},
	}
	handler := slog.NewTextHandler(os.Stdout, handlerOpts)
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
