package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

var ErrNoSuchPage = errors.New("no such page")

const PageSize = 4096

type Manager struct {
	mu sync.RWMutex

	fileIDToPath map[common.FileID]string
	newPageFunc  func(fileID common.FileID, pageID common.PageID) *page.SlottedPage
}

var (
	_ common.DiskManager[*page.SlottedPage] = &Manager{}
)

func New(
	fileIDToPath map[common.FileID]string,
	newPageFunc func(fileID common.FileID, pageID common.PageID) *page.SlottedPage,
) *Manager {
	return &Manager{
		fileIDToPath: fileIDToPath,
		newPageFunc:  newPageFunc,
	}
}

func (m *Manager) Lock() {
	m.mu.Lock()
}

func (m *Manager) Unlock() {
	m.mu.Unlock()
}

func (m *Manager) ReadPage(pg *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return err
	}

	page := m.newPageFunc(pageIdent.FileID, pageIdent.PageID)

	page.SetData(data)
	page.UnsafeInitLatch()
	return nil
}

func (m *Manager) GetPageNoNew(page *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * PageSize)
	data := make([]byte, PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return errors.Join(err, ErrNoSuchPage)
	}

	page.SetData(data)
	return nil
}

func (m *Manager) WritePageAssumeLocked(
	lockedPage *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	path, ok := m.fileIDToPath[pageIdent.FileID]
	if !ok {
		return fmt.Errorf("fileID %d not found in path map", pageIdent.FileID)
	}

	data := lockedPage.GetData()
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

type InMemoryManager struct {
	mu    sync.RWMutex
	pages map[common.PageIdentity]*page.SlottedPage
}

var (
	_ common.DiskManager[*page.SlottedPage] = &InMemoryManager{}
)

func NewInMemoryManager() *InMemoryManager {
	return &InMemoryManager{
		mu:    sync.RWMutex{},
		pages: make(map[common.PageIdentity]*page.SlottedPage),
	}
}

func (m *InMemoryManager) GetPageNoNew(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	storedPage, ok := m.pages[pageIdent]
	if !ok {
		return ErrNoSuchPage
	}

	pg.SetData(storedPage.GetData())
	pg.UnsafeInitLatch()
	return nil
}

func (m *InMemoryManager) ReadPage(pg *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	storedPage, ok := m.pages[pageIdent]
	if !ok {
		m.pages[pageIdent] = page.NewSlottedPage()
		pg.SetData(m.pages[pageIdent].GetData())
		return nil
	}

	storedPage.UnsafeInitLatch()
	pg.SetData(storedPage.GetData())
	return nil
}

func (m *InMemoryManager) Lock() {
	m.mu.Lock()
}

func (m *InMemoryManager) Unlock() {
	m.mu.Unlock()
}

func (m *InMemoryManager) WritePageAssumeLocked(
	pg *page.SlottedPage,
	pgIdent common.PageIdentity,
) error {
	if _, ok := m.pages[pgIdent]; !ok {
		return fmt.Errorf("page %+v not found", pgIdent)
	}

	m.pages[pgIdent].SetData(pg.GetData())
	return nil
}
