package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type Logger struct {
	json  bool
	level string
	out   io.Writer
}

func New(level string, jsonOutput bool) *Logger {
	if strings.TrimSpace(level) == "" {
		level = "info"
	}
	return &Logger{level: strings.ToLower(level), json: jsonOutput, out: os.Stderr}
}

func (l *Logger) Info(msg string, fields ...any) {
	l.write("info", msg, fields...)
}

func (l *Logger) Error(msg string, fields ...any) {
	l.write("error", msg, fields...)
}

func (l *Logger) Debug(msg string, fields ...any) {
	if l.level == "debug" {
		l.write("debug", msg, fields...)
	}
}

func (l *Logger) write(level, msg string, fields ...any) {
	if l == nil {
		log.Printf("%s: %s", level, msg)
		return
	}
	if l.json {
		m := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339), "level": level, "msg": msg}
		for i := 0; i+1 < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if ok {
				m[key] = fields[i+1]
			}
		}
		_ = json.NewEncoder(l.out).Encode(m)
		return
	}
	if len(fields) > 0 {
		fmt.Fprintf(l.out, "%s %-5s %s %v\n", time.Now().Format(time.RFC3339), strings.ToUpper(level), msg, fields)
		return
	}
	fmt.Fprintf(l.out, "%s %-5s %s\n", time.Now().Format(time.RFC3339), strings.ToUpper(level), msg)
}
