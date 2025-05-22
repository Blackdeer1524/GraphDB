package bufferpool

import (
	"container/list"
	"errors"
	"sync"
)

type LRUReplacer struct {
	mu     sync.Mutex
	lru    *list.List
	frames map[uint64]*list.Element
}

func NewLRUReplacer() *LRUReplacer {
	return &LRUReplacer{
		lru:    list.New(),
		frames: make(map[uint64]*list.Element),
	}
}

func (l *LRUReplacer) Pin(frameID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.frames[frameID]; ok {
		l.lru.Remove(elem)
		delete(l.frames, frameID)
	}
}

func (l *LRUReplacer) Unpin(frameID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.frames[frameID]; exists {
		return // уже в LRU
	}

	elem := l.lru.PushFront(frameID)
	l.frames[frameID] = elem
}

func (l *LRUReplacer) ChooseVictim() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	elem := l.lru.Back()
	if elem == nil {
		return 0, errors.New("no victim available")
	}

	frameID := elem.Value.(uint64)

	l.lru.Remove(elem)
	delete(l.frames, frameID)

	return frameID, nil
}

func (l *LRUReplacer) GetSize() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return uint64(len(l.frames))
}
