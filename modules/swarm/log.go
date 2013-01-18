package swarm

import (
	"fmt"
	"log"
	"os"
)

const (
	FATAL = iota
	CRITICAL
	ERROR
	WARN
	INFO
	DEBUG
)

type Logger struct {
	LogFileName string
}

var levelName map[int]string

func InitLog(logFileName string) (logger Logger) {
	levelName = map[int]string{
		FATAL:    "FATAL",
		CRITICAL: "CRITICAL",
		ERROR:    "ERROR",
		WARN:     "WARN",
		INFO:     "INFO",
		DEBUG:    "DEBUG",
	}

	LogFile, err := os.OpenFile(logFileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic("Error open log file: " + logFileName)
	}
	log.SetOutput(LogFile)
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("")

	logger = Logger{
		LogFileName: logFileName,
	}
	return logger
}

func _log(logLevel int, format string, v ...interface{}) {
	if logLevel <= LOG_LEVEL {
		msg := fmt.Sprintf(format, v...)
		log.Println("["+levelName[logLevel]+"]:", msg)
	}
}

func (this *Logger) Fatal(format string, v ...interface{}) {
	_log(FATAL, format, v...)
	_log(ERROR, stack())
}

func (this *Logger) Critical(format string, v ...interface{}) {
	_log(CRITICAL, format, v...)
}

func (this *Logger) Error(format string, v ...interface{}) {
	_log(ERROR, format, v...)
}

func (this *Logger) Warn(format string, v ...interface{}) {
	_log(WARN, format, v...)
}

func (this *Logger) Info(format string, v ...interface{}) {
	_log(INFO, format, v...)
}

func (this *Logger) Debug(format string, v ...interface{}) {
	_log(DEBUG, format, v...)
}
