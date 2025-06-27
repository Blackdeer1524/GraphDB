package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const PageSize = 4096

type Manager[T Page] struct {
	fileIDToPath map[uint64]string
	newPageFunc  func(fileID, pageID uint64) T
}

type Page interface {
	GetData() []byte
	SetData(d []byte)

	SetDirtiness(val bool)
	IsDirty() bool

	GetFileID() uint64
	GetPageID() uint64

	// latch methods
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

func New[T Page](
	fileIDToPath map[uint64]string,
	newPageFunc func(fileID, pageID uint64) T,
) *Manager[T] {
	return &Manager[T]{
		fileIDToPath: fileIDToPath,
		newPageFunc:  newPageFunc,
	}
}

func (m *Manager[T]) ReadPage(fileID, pageID uint64) (T, error) {
	var zeroVal T

	path, ok := m.fileIDToPath[fileID]
	if !ok {
		return zeroVal, fmt.Errorf("fileID %d not found in path map", fileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return zeroVal, err
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return zeroVal, err
	}

	page := m.newPageFunc(fileID, pageID)

	page.SetData(data)

	return page, nil
}

func (m *Manager[T]) GetPageNoNew(page T, fileID, pageID uint64) (T, error) {
	var zeroVal T

	path, ok := m.fileIDToPath[fileID]
	if !ok {
		return zeroVal, fmt.Errorf("fileID %d not found in path map", fileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return zeroVal, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return zeroVal, fmt.Errorf("failed to reat at: %w", err)
	}

	page.SetData(data)

	return page, nil
}

func (m *Manager[T]) WritePage(page *T) error {
	fileID := (*page).GetFileID()
	pageID := (*page).GetPageID()

	path, ok := m.fileIDToPath[fileID]
	if !ok {
		return fmt.Errorf("fileID %d not found in path map", fileID)
	}

	data := (*page).GetData()
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
	offset := int64(pageID * PageSize)

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write at file %s: %w", path, err)
	}

	(*page).SetDirtiness(false)

	return nil
}
