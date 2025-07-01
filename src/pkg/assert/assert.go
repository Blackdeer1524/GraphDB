package assert

import (
	"fmt"
	"path/filepath"
	"runtime"
)

func Assert(condition bool, args ...any) {
	if condition {
		return
	}

	// Get caller info (skip 1 frame to get the caller of Assert)
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	// Shorten file path to just the filename
	filename := filepath.Base(file)

	if len(args) > 0 {
		format := args[0].(string)
		message := fmt.Sprintf(format, args[1:]...)
		m := fmt.Sprintf("Assertion failed: %s at %s:%d\n", message, filename, line)
		panic(m)
	}
	m := fmt.Sprintf("Assertion failed at %s:%d\n", filename, line)
	panic(m)
}
