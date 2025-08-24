package bufferpool

import (
	"container/list"
	"errors"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type LRUReplacer struct {
	mu     sync.Mutex
	lru    *list.List
	frames map[common.PageIdentity]*list.Element
}

var (
	_ Replacer = &LRUReplacer{}
)

func NewLRUReplacer() *LRUReplacer {
	return &LRUReplacer{
		lru:    list.New(),
		frames: make(map[common.PageIdentity]*list.Element),
	}
}

func (l *LRUReplacer) Pin(frameID common.PageIdentity) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if elem, ok := l.frames[frameID]; ok {
		l.lru.Remove(elem)
		delete(l.frames, frameID)
	}
}

func (l *LRUReplacer) Unpin(frameID common.PageIdentity) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, exists := l.frames[frameID]; exists {
		return // уже в LRU
	}

	elem := l.lru.PushFront(frameID)
	l.frames[frameID] = elem
}

func (l *LRUReplacer) ChooseVictim() (common.PageIdentity, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	elem := l.lru.Back()
	if elem == nil {
		return common.PageIdentity{}, errors.New("no victim available")
	}

	frameID := elem.Value.(common.PageIdentity)

	l.lru.Remove(elem)
	delete(l.frames, frameID)

	return frameID, nil
}

func (l *LRUReplacer) GetSize() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	return uint64(len(l.frames))
}
