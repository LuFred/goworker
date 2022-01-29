package logger

import "go.uber.org/zap"

func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	logger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

// Debug is debug level
func Debug(msg string, fields ...zap.Field) {
	logger.Debug(msg, fields...)
}
