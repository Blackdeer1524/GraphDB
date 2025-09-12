package disk

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

var ErrNoSuchPage = errors.New("no such page")

type Manager struct {
	basePath    string
	mu          sync.Mutex
	newPageFunc func(fileID common.FileID, pageID common.PageID) *page.SlottedPage

	fs afero.Fs
}

var (
	_ common.DiskManager[*page.SlottedPage] = &Manager{}
)

func New(
	basePath string,
	newPageFunc func(fileID common.FileID, pageID common.PageID) *page.SlottedPage,
	fs afero.Fs,
) *Manager {
	return &Manager{
		basePath:    basePath,
		newPageFunc: newPageFunc,
		mu:          sync.Mutex{},
		fs:          fs,
	}
}
func (m *Manager) Lock() {
	m.mu.Lock()
}

func (m *Manager) Unlock() {
	m.mu.Unlock()
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
	path := utils.GetFilePath(m.basePath, pageIdent.FileID)

	file, err := m.fs.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	//nolint:gosec
	offset := int64(pageIdent.PageID * page.PageSize)
	data := make([]byte, page.PageSize)

	_, err = file.ReadAt(data, offset)
	if errors.Is(err, io.EOF) {
		newPage := m.newPageFunc(pageIdent.FileID, pageIdent.PageID)
		_, err := file.WriteAt(newPage.GetData(), offset)
		if err != nil {
			return err
		}

		err = file.Sync()
		if err != nil {
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
	path := utils.GetFilePath(m.basePath, pageIdent.FileID)

	file, err := m.fs.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

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
	path := utils.GetFilePath(m.basePath, pageIdent.FileID)

	data := lockedPage.GetData()
	if len(data) == 0 {
		return errors.New("page data is empty")
	}

	file, err := m.fs.OpenFile(
		filepath.Clean(path),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", path, err)
	}
	defer file.Close()

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
		m.pages[pageIdent] = page.NewSlottedPage()
		pg.SetData(m.pages[pageIdent].GetData())
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

	m.pages[pgIdent].SetData(pg.GetData())
	return nil
}

func (m *Manager) GetLastFilePage(fileID common.FileID) (common.PageID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := utils.GetFilePath(m.basePath, fileID)

	file, err := m.fs.OpenFile(filepath.Clean(path), os.O_RDWR, 0o644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	filesize := info.Size()
	pagesCount := filesize / page.PageSize
	if pagesCount == 0 {
		return 0, ErrNoSuchPage
	}
	return common.PageID(pagesCount - 1), nil
}

func (m *Manager) GetEmptyPage(fileID common.FileID) (common.PageID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := utils.GetFilePath(m.basePath, fileID)

	file, err := m.fs.OpenFile(filepath.Clean(path), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	filesize := info.Size()
	pagesCount := filesize / page.PageSize
	return common.PageID(pagesCount), nil
}
