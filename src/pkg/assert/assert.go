package assert

import (
	"fmt"
	"path/filepath"
	"runtime"
)

func Assert(condition bool, format string, args ...any) {
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

	// Format the custom message
	message := fmt.Sprintf(format, args...)

	// Print error with source location
	m := fmt.Sprintf("Assertion failed: %s at %s:%d\n", message, filename, line)
	panic(m)
}
