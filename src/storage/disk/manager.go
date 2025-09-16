package disk

import (
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

var ErrNoSuchPage = errors.New("no such page")

type Manager struct {
	basePath    string
	mu          sync.Mutex
	newPageFunc func(fileID common.FileID, pageID common.PageID) page.SlottedPage

	fs        afero.Fs
	fileCache map[common.FileID]afero.File
	cacheMu   sync.RWMutex
}

var (
	_ common.DiskManager[*page.SlottedPage] = &Manager{}
)

func New(
	basePath string,
	newPageFunc func(fileID common.FileID, pageID common.PageID) page.SlottedPage,
	fs afero.Fs,
) *Manager {
	return &Manager{
		basePath:    basePath,
		newPageFunc: newPageFunc,
		mu:          sync.Mutex{},
		fs:          fs,
		fileCache:   make(map[common.FileID]afero.File),
		cacheMu:     sync.RWMutex{},
	}
}
func (m *Manager) Lock() {
	m.mu.Lock()
}

func (m *Manager) Unlock() {
	m.mu.Unlock()
}

// getOrOpenFile returns a cached file handle or opens a new one and caches it
func (m *Manager) getOrOpenFile(fileID common.FileID) (afero.File, error) {
	m.cacheMu.RLock()
	if file, exists := m.fileCache[fileID]; exists {
		m.cacheMu.RUnlock()
		return file, nil
	}
	m.cacheMu.RUnlock()

	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	if file, exists := m.fileCache[fileID]; exists {
		m.cacheMu.RUnlock()
		return file, nil
	}

	path := utils.GetFilePath(m.basePath, fileID)
	file, err := m.fs.OpenFile(
		filepath.Clean(path),
		os.O_RDWR|os.O_CREATE,
		0o644,
	)
	if err != nil {
		return nil, err
	}

	m.fileCache[fileID] = file
	return file, nil
}

func (m *Manager) ReadPage(pg *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ReadPageAssumeLocked(pg, pageIdent)
}

func (m *Manager) ReadPageAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	file, err := m.getOrOpenFile(pageIdent.FileID)
	if err != nil {
		return err
	}

	//nolint:gosec
	offset := int64(pageIdent.PageID * page.PageSize)
	data := make([]byte, page.PageSize)

	_, err = file.ReadAt(data, offset)
	if errors.Is(err, io.EOF) {
		newPage := m.newPageFunc(pageIdent.FileID, pageIdent.PageID)
		_, err := file.WriteAt(newPage[:], offset)
		if err != nil {
			return err
		}

		err = file.Sync()
		if err != nil {
			path := utils.GetFilePath(m.basePath, pageIdent.FileID)
			return fmt.Errorf("failed to sync file %s: %w", path, err)
		}

		pg.SetData(newPage.GetData())
		return nil
	} else if err != nil {
		return err
	}

	pg.SetData(data)
	pg.UnsafeInitLatch()
	return nil
}

func (m *Manager) GetPageNoNew(pg *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.GetPageNoNewAssumeLocked(pg, pageIdent)
}

func (m *Manager) GetPageNoNewAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	file, err := m.getOrOpenFile(pageIdent.FileID)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	//nolint:gosec
	offset := int64(pageIdent.PageID * page.PageSize)
	data := make([]byte, page.PageSize)

	_, err = file.ReadAt(data, offset)
	if err != nil {
		return errors.Join(err, ErrNoSuchPage)
	}

	pg.SetData(data)
	pg.UnsafeInitLatch()
	return nil
}

func (m *Manager) WritePageAssumeLocked(
	lockedPage *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	data := lockedPage[:]
	path := utils.GetFilePath(m.basePath, pageIdent.FileID)

	file, err := m.getOrOpenFile(pageIdent.FileID)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}

	//nolint:gosec
	offset := int64(pageIdent.PageID * page.PageSize)

	_, err = file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write at file %s: %w", path, err)
	}
	err = file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file %s: %w", path, err)
	}

	return nil
}

type InMemoryManager struct {
	mu    sync.Mutex
	pages map[common.PageIdentity]*page.SlottedPage
}

var (
	_ common.DiskManager[*page.SlottedPage] = &InMemoryManager{}
)

func NewInMemoryManager() *InMemoryManager {
	return &InMemoryManager{
		mu:    sync.Mutex{},
		pages: make(map[common.PageIdentity]*page.SlottedPage),
	}
}

func (m *InMemoryManager) Lock() {
	m.mu.Lock()
}

func (m *InMemoryManager) Unlock() {
	m.mu.Unlock()
}

func (m *InMemoryManager) GetPageNoNew(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.GetPageNoNewAssumeLocked(pg, pageIdent)
}

func (m *InMemoryManager) GetPageNoNewAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	storedPage, ok := m.pages[pageIdent]
	if !ok {
		return ErrNoSuchPage
	}

	pg.SetData(storedPage.GetData())
	pg.UnsafeInitLatch()
	return nil
}

func (m *InMemoryManager) ReadPage(pg *page.SlottedPage, pageIdent common.PageIdentity) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ReadPageAssumeLocked(pg, pageIdent)
}

func (m *InMemoryManager) ReadPageAssumeLocked(
	pg *page.SlottedPage,
	pageIdent common.PageIdentity,
) error {
	storedPage, ok := m.pages[pageIdent]
	if !ok {
		newPage := page.NewSlottedPage()
		m.pages[pageIdent] = &newPage
		pg.SetData(newPage[:])
		return nil
	}

	pg.SetData(storedPage.GetData())
	pg.UnsafeInitLatch()
	return nil
}

func (m *InMemoryManager) WritePageAssumeLocked(
	pg *page.SlottedPage,
	pgIdent common.PageIdentity,
) error {
	if _, ok := m.pages[pgIdent]; !ok {
		return fmt.Errorf("page %+v not found", pgIdent)
	}

	m.pages[pgIdent].SetData(pg[:])
	return nil
}

func (m *Manager) GetLastFilePage(fileID common.FileID) (common.PageID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := m.getOrOpenFile(fileID)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	filesize := info.Size()
	pagesCount := filesize / page.PageSize
	if pagesCount == 0 {
		return 0, ErrNoSuchPage
	}
	//nolint:gosec
	return common.PageID(pagesCount - 1), nil
}

func (m *Manager) GetEmptyPage(fileID common.FileID) (common.PageID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	file, err := m.getOrOpenFile(fileID)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	filesize := info.Size()
	pagesCount := filesize / page.PageSize
	//nolint:gosec
	return common.PageID(pagesCount), nil
}

// BulkWritePageAssumeLockedBegin returns two handles for batched page writes.
// The first handle writes a page at the given pageID offset without flushing.
// The second handle must be called by the caller to flush the file contents.
func (m *Manager) BulkWritePageAssumeLockedBegin(
	fileID common.FileID,
) (func(lockedPage *page.SlottedPage, pageID common.PageID) error, func() error, error) {
	file, err := m.getOrOpenFile(fileID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}

	path := utils.GetFilePath(m.basePath, fileID)

	bulkWriter := func(lockedPage *page.SlottedPage, pageID common.PageID) error {
		data := lockedPage[:]
		//nolint:gosec
		offset := int64(pageID * page.PageSize)

		_, err := file.WriteAt(data, offset)
		if err != nil {
			return fmt.Errorf("failed to write at file %s: %w", path, err)
		}
		return nil
	}

	doneHandle := func() error {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("failed to sync file %s: %w", path, err)
		}
		return nil
	}

	return bulkWriter, doneHandle, nil
}

func (m *InMemoryManager) BulkWritePageAssumeLockedBegin(
	fileID common.FileID,
) (func(lockedPage *page.SlottedPage, pageID common.PageID) error, func() error, error) {
	bulkWriter := func(lockedPage *page.SlottedPage, pageID common.PageID) error {
		pageIdent := common.PageIdentity{
			FileID: fileID,
			PageID: pageID,
		}

		if existingPage, exists := m.pages[pageIdent]; exists {
			existingPage.SetData(lockedPage[:])
		} else {
			newPage := page.NewSlottedPage()
			newPage.SetData(lockedPage[:])
			m.pages[pageIdent] = &newPage
		}

		return nil
	}

	doneHandle := func() error {
		return nil
	}

	return bulkWriter, doneHandle, nil
}
