package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

const PageSize = 4096

type Manager[T Page] struct {
	fileIDToPath map[common.FileID]string
	newPageFunc  func() T

	mu *sync.RWMutex
}

type Page interface {
	GetData() []byte
	SetData(d []byte)

	Lock()
	Unlock()
	RLock()
	RUnlock()
}

func New[T Page](
	fileIDToPath map[common.FileID]string,
	newPageFunc func() T,
) *Manager[T] {
	return &Manager[T]{
		fileIDToPath: fileIDToPath,
		newPageFunc:  newPageFunc,

		mu: new(sync.RWMutex),
	}
}

func (m *Manager[T]) ReadPage(pageIdent common.PageIdentity) (T, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var zeroVal T

	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return zeroVal, fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return zeroVal, err
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return zeroVal, err
	}

	page := m.newPageFunc()

	page.SetData(data)

	return page, nil
}

func (m *Manager[T]) GetPageNoNew(page T, pageIdent common.PageIdentity) (T, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var zeroVal T

	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return zeroVal, fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return zeroVal, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return zeroVal, fmt.Errorf("failed to reat at: %w", err)
	}

	page.SetData(data)

	return page, nil
}

func (m *Manager[T]) WritePage(page T, pageIdent common.PageIdentity) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	data := page.GetData()
	if len(data) == 0 {
		return errors.New("page data is empty")
	}

	file, err := os.OpenFile(
		filepath.Clean(path),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * PageSize)

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write at file %s: %w", path, err)
	}

	return nil
}

func (m *Manager[T]) UpdateFileMap(mp map[common.FileID]string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fileIDToPath = mp
}

func (m *Manager[T]) InsertToFileMap(id common.FileID, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fileIDToPath[id] = path
}
