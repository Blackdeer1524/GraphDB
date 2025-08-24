package dbg

import (
	"github.com/petermattis/goid"
	"log"
)

func LogWithGoid(msg string, v ...any) {
	log.Printf("goid=%d, "+msg, goid.Get(), v)
}
