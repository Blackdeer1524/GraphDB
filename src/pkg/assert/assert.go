package assert

import (
	"fmt"
	"path/filepath"
	"runtime"
)

func assertWithDepth(condition bool, depth int, args ...any) bool {
	if condition {
		return true
	}

	// Get caller info (skip 1 frame to get the caller of Assert)
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		file = "unknown"
		line = 0
	}

	// Shorten file path to just the filename
	filename := filepath.Base(file)

	if len(args) > 0 {
		format := args[0].(string)
		message := fmt.Sprintf(format, args[1:]...)
		m := fmt.Sprintf(
			"Assertion failed: %s at %s:%d\n",
			message,
			filename,
			line,
		)
		panic(m)
	}
	m := fmt.Sprintf("Assertion failed at %s:%d\n", filename, line)
	panic(m)
}

func Assert(condition bool, args ...any) bool {
	return assertWithDepth(condition, 2, args...)
}

func NoError(err error) bool {
	return assertWithDepth(err == nil, 2, "expected no error, got: %v", err)
}

func NoErrorWithMessage(err error, args ...any) bool {
	return assertWithDepth(err == nil, 2, args...)
}

// Cast attempts to cast the provided value 'data' to the specified
// type 'T'. If the cast is not possible, it triggers an assertion failure with
// an error message.
//
// Example usage:
//
//	value := Cast[int](someAnyValue)
//
// Panics:
//
//	Panics if 'data' cannot be cast to type 'T'.
func Cast[T any](data any) T {
	castedData, ok := data.(T)
	assertWithDepth(ok, 2, "couldn't perform a type cast")
	return castedData
}
