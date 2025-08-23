package dbg

import (
	"github.com/petermattis/goid"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

type LoggedMutex struct {
	mu   *sync.Mutex
	name string
}

func getCaller() string {
	skip := 3

	// Максимум 32 фрейма — можно увеличить
	pc := make([]uintptr, 32)
	n := runtime.Callers(skip, pc)
	if n == 0 {
		return "unknown"
	}

	var callers []string
	frames := runtime.CallersFrames(pc[:n])

	for {
		frame, more := frames.Next()
		fn := frame.Func
		if fn != nil {
			// Берём только имя функции (без полного пути пакета)
			name := filepath.Base(fn.Name())
			callers = append(callers, name)
		} else {
			callers = append(callers, "unknown")
		}
		if !more {
			break
		}
	}

	return strings.Join(callers, " → ")
}

func NewLoggedMutex(name string) *LoggedMutex {
	return &LoggedMutex{
		mu:   new(sync.Mutex),
		name: name,
	}
}

func (lm *LoggedMutex) Lock() {
	log.Printf("%d trying to lock %s, caller=%s", goid.Get(), lm.name, getCaller())

	lm.mu.Lock()

	//log.Printf("%d locked %s, caller=%s", goid.Get(), lm.name, getCaller())
	LogWithGoid("locked %s, caller=%s", lm.name, getCaller())
}

func (lm *LoggedMutex) Unlock() {
	lm.mu.Unlock()

	LogWithGoid("unlocked %s, caller=%s", lm.name, getCaller())
}
