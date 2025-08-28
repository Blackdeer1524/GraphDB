package systemcatalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/afero"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

const (
	currentVersionFile = "CURRENT"
	zeroVersion        = uint64(0)

	catalogVersionFileID  = common.FileID(0)
	catalogVersionPageID  = common.PageID(0)
	catalogVersionSlotNum = uint16(0)
)

var (
	ErrEntityNotFound = errors.New("entity not found")
	ErrEntityExists   = errors.New("entity already exists")
)

// catalogVersionPageIdent return page identity of page with current version of system catalog.
// We reserve zero fileID and zero pageID for this page.
func catalogVersionPageIdent() common.PageIdentity {
	return common.PageIdentity{FileID: catalogVersionFileID, PageID: catalogVersionPageID}
}

func catalogVersionPageRecordID() common.RecordID {
	return common.RecordID{
		FileID:  catalogVersionFileID,
		PageID:  catalogVersionPageID,
		SlotNum: catalogVersionSlotNum,
	}
}

type Data struct {
	Metadata     storage.Metadata               `json:"metadata"`
	VertexTables map[string]storage.VertexTable `json:"vertex_tables"`
	EdgeTables   map[string]storage.EdgeTable   `json:"edge_tables"`
	Indexes      map[string]storage.Index       `json:"indexes"`
}

type Manager struct {
	fs        afero.Fs
	basePath  string
	data      *Data
	maxFileID uint64

	bp                 bufferpool.BufferPool
	currentVersionPage *page.SlottedPage

	// currentVersion uses for cache if version from file is equal to
	// this then we don't need to reread it from disk
	currentVersion uint64

	mu *sync.RWMutex
}

func GetSystemCatalogVersionFileName(basePath string) string {
	return filepath.Join(basePath, currentVersionFile)
}

func getSystemCatalogFilename(basePath string, v uint64) string {
	return filepath.Join(basePath, "system_catalog_"+fmt.Sprint(v)+".json")
}

func isFileExists(fs afero.Fs, path string) (bool, error) {
	_, err := fs.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func initializeVersionFile(fs afero.Fs, versionFile string) (err error) {
	p := page.NewSlottedPage()

	slotOpt := p.UnsafeInsertNoLogs(utils.ToBytes(zeroVersion))
	assert.Assert(slotOpt.IsSome())

	data := p.GetData()

	file, err := fs.OpenFile(
		filepath.Clean(versionFile),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open current version file: %w", err)
	}
	defer func() {
		err = errors.Join(err, file.Close())
	}()

	_, err = file.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to write at file: %w", err)
	}

	err = file.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// InitSystemCatalog - initializes system catalogs if files not exist.
// If there is no current version file then we have to create it
// and fill it with zeroVersion and create default system catalog file.
func InitSystemCatalog(basePath string, fs afero.Fs) error {
	versionFile := GetSystemCatalogVersionFileName(basePath)

	ok, err := isFileExists(fs, versionFile)
	if err != nil {
		return fmt.Errorf("failed to check existence of current version file: %w", err)
	}

	if ok {
		return nil
	}

	err = initializeVersionFile(fs, versionFile)
	if err != nil {
		return fmt.Errorf("failed to initialize version file: %w", err)
	}

	scFilename := getSystemCatalogFilename(basePath, zeroVersion)

	d := Data{
		VertexTables: map[string]storage.VertexTable{},
		EdgeTables:   map[string]storage.EdgeTable{},
		Indexes:      map[string]storage.Index{},
	}

	data, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("failed to marshal to json: %w", err)
	}

	file, err := fs.OpenFile(
		filepath.Clean(scFilename),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return fmt.Errorf("failed to open current version file: %w", err)
	}
	defer func() {
		cerr := file.Close()
		if cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()

	_, err = file.WriteAt(data, 0)
	if err != nil {
		return fmt.Errorf("failed to write at file: %w", err)
	}

	return nil
}

// New creates new system catalog manager. It reads current version from current version file
// and reads system catalog file with this version. Also, it allocates page for current version.
// Page is used for concurrency control.
func New(basePath string, fs afero.Fs, bp bufferpool.BufferPool) (*Manager, error) {
	versionFile := GetSystemCatalogVersionFileName(basePath)

	ok, err := isFileExists(fs, versionFile)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence of current version file: %w", err)
	}

	if !ok {
		return nil, fmt.Errorf(
			"current version file %q not found; run InitSystemCatalog first",
			versionFile,
		)
	}

	cvp, err := bp.GetPage(catalogVersionPageIdent())
	if err != nil {
		return nil, fmt.Errorf("failed to get page with version: %w", err)
	}

	// current version page stores only one slot with current version of system catalog
	versionNum := utils.FromBytes[uint64](cvp.LockedRead(catalogVersionSlotNum))

	sysCatFilename := getSystemCatalogFilename(basePath, versionNum)

	dataBytes, err := afero.ReadFile(fs, sysCatFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to read system catalog file: %w", err)
	}

	var data Data

	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal system catalog file: %w", err)
	}

	return &Manager{
		bp:                 bp,
		currentVersionPage: cvp,
		currentVersion:     versionNum,
		basePath:           basePath,
		data:               &data,
		fs:                 fs,
		maxFileID:          calcMaxFileID(&data),

		mu: new(sync.RWMutex),
	}, nil
}

func (m *Manager) updateSystemCatalogData() error {
	m.mu.RLock()
	versionNum := utils.FromBytes[uint64](m.currentVersionPage.LockedRead(catalogVersionSlotNum))
	if m.currentVersion == versionNum {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	versionNum = utils.FromBytes[uint64](m.currentVersionPage.LockedRead(catalogVersionSlotNum))
	if m.currentVersion == versionNum {
		return nil
	}

	sysCatFilename := getSystemCatalogFilename(m.basePath, versionNum)
	dataBytes, err := afero.ReadFile(m.fs, sysCatFilename)
	if err != nil {
		return fmt.Errorf("failed to read system catalog file: %w", err)
	}

	var data Data

	err = json.Unmarshal(dataBytes, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal system catalog file: %w", err)
	}

	m.data = &data
	m.currentVersion = versionNum

	return nil
}

func (m *Manager) GetBasePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.basePath
}

func (m *Manager) Save(logger common.ITxnLoggerWithContext) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	nVersion := m.currentVersion + 1

	var data []byte

	data, err = json.Marshal(m.data)
	if err != nil {
		return fmt.Errorf("failed to marshal system catalog data: %w", err)
	}

	sysCatFilename := getSystemCatalogFilename(m.basePath, nVersion)

	file, err := m.fs.OpenFile(filepath.Clean(sysCatFilename),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open system catalog file: %w", err)
	}

	_, err = file.Write(data)
	if err != nil {
		err1 := file.Close()
		if err1 != nil {
			err = errors.Join(err, err1)
		}

		return fmt.Errorf("failed to write system catalog file: %w", err)
	}

	err = file.Sync()
	if err != nil {
		err1 := file.Close()
		if err1 != nil {
			err = errors.Join(err, err1)
		}

		return fmt.Errorf("failed to sync system catalog file: %w", err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("failed to close system catalog file: %w", err)
	}

	err = m.bp.WithMarkDirty(
		logger.GetTxnID(),
		catalogVersionPageIdent(),
		m.currentVersionPage,
		func(lockedPage *page.SlottedPage) (common.LogRecordLocInfo, error) {
			loc, err := lockedPage.UpdateWithLogs(
				utils.ToBytes(nVersion),
				catalogVersionPageRecordID(),
				logger,
			)
			if err != nil {
				return common.NewNilLogRecordLocation(), err
			}
			m.currentVersion++
			return loc, nil
		},
	)
	return err
}

func (m *Manager) GetNewFileID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maxFileID++
	return m.maxFileID
}

func (m *Manager) GetVertexTableMeta(name string) (storage.VertexTable, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return storage.VertexTable{}, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.data.VertexTables[name]
	if !exists {
		return storage.VertexTable{}, ErrEntityNotFound
	}

	return table, nil
}

func (m *Manager) VertexTableExists(name string) (bool, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return false, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.VertexTables[name]

	return exists, nil
}

func (m *Manager) AddVertexTable(req storage.VertexTable) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.VertexTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.VertexTables[req.Name] = req

	return nil
}

func (m *Manager) DropVertexTable(name string) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.VertexTables, name)

	return nil
}

func (m *Manager) VertexGetIndexes(name string) ([]storage.Index, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return nil, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]storage.Index, 0)

	for _, index := range m.data.Indexes {
		if index.TableName == name && index.TableKind == "vertex" {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) GetEdgeTableMeta(name string) (storage.EdgeTable, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return storage.EdgeTable{}, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.data.EdgeTables[name]
	if !exists {
		return storage.EdgeTable{}, ErrEntityNotFound
	}

	return table, nil
}

func (m *Manager) EdgeTableExists(name string) (bool, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return false, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.EdgeTables[name]

	return exists, nil
}

func (m *Manager) AddEdgeTable(req storage.EdgeTable) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.EdgeTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.EdgeTables[req.Name] = req

	return nil
}

func (m *Manager) DropEdgeTable(name string) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.EdgeTables, name)

	return nil
}

func (m *Manager) EdgeGetIndexes(name string) ([]storage.Index, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return nil, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]storage.Index, 0)

	for _, index := range m.data.Indexes {
		if index.TableName == name && index.TableKind == "edge" {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) IndexExists(name string) (bool, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return false, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.Indexes[name]

	return exists, nil
}

func (m *Manager) GetIndexMeta(name string) (storage.Index, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return storage.Index{}, fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.data.Indexes[name]
	if !exists {
		return storage.Index{}, ErrEntityNotFound
	}

	return index, nil
}

func (m *Manager) AddIndex(index storage.Index) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.Indexes[index.Name]; exists {
		return ErrEntityExists
	}

	_, ok1 := m.data.VertexTables[index.TableName]
	_, ok2 := m.data.EdgeTables[index.TableName]

	if !ok1 && !ok2 {
		return fmt.Errorf("table %s not found", index.TableName)
	}

	m.data.Indexes[index.Name] = index

	return nil
}

func (m *Manager) DropIndex(name string) error {
	err := m.updateSystemCatalogData()
	if err != nil {
		return fmt.Errorf("failed to update system catalog data: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.Indexes[name]; !exists {
		return ErrEntityNotFound
	}

	delete(m.data.Indexes, name)

	return nil
}

func (m *Manager) CopyData() (*Data, error) {
	err := m.updateSystemCatalogData()
	if err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	metadata := storage.Metadata{
		Version: m.data.Metadata.Version,
		Name:    m.data.Metadata.Name,
	}

	vertexTables := make(map[string]storage.VertexTable, len(m.data.VertexTables))
	for name, table := range m.data.VertexTables {
		properties := make(map[string]storage.Column, len(table.Schema))
		for k, v := range table.Schema {
			properties[k] = v
		}

		vertexTables[name] = storage.VertexTable{
			Name:       table.Name,
			PathToFile: table.PathToFile,
			FileID:     table.FileID,
			Schema:     properties,
		}
	}

	edgeTables := make(map[string]storage.EdgeTable, len(m.data.EdgeTables))
	for name, table := range m.data.EdgeTables {
		properties := make(map[string]storage.Column, len(table.Schema))
		for k, v := range table.Schema {
			properties[k] = v
		}

		edgeTables[name] = storage.EdgeTable{
			Name:       table.Name,
			PathToFile: table.PathToFile,
			FileID:     table.FileID,
			Schema:     properties,
		}
	}

	indexes := make(map[string]storage.Index, len(m.data.Indexes))
	for name, index := range m.data.Indexes {
		columns := make([]string, len(index.Columns))
		copy(columns, index.Columns)

		indexes[name] = storage.Index{
			Name:        index.Name,
			PathToFile:  index.PathToFile,
			FileID:      index.FileID,
			TableName:   index.TableName,
			TableKind:   index.TableKind,
			Columns:     columns,
			KeyBytesCnt: index.KeyBytesCnt,
		}
	}

	return &Data{
		Metadata:     metadata,
		VertexTables: vertexTables,
		EdgeTables:   edgeTables,
		Indexes:      indexes,
	}, nil
}

func (m *Manager) GetFileIDToPathMap() map[common.FileID]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mp := make(map[common.FileID]string)

	for _, v := range m.data.Indexes {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.EdgeTables {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.VertexTables {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	return mp
}

func calcMaxFileID(data *Data) uint64 {
	maxFileID := uint64(0)

	for _, v := range data.VertexTables {
		if v.FileID > maxFileID {
			maxFileID = v.FileID
		}
	}

	for _, v := range data.EdgeTables {
		if v.FileID > maxFileID {
			maxFileID = v.FileID
		}
	}

	for _, v := range data.Indexes {
		if v.FileID > maxFileID {
			maxFileID = v.FileID
		}
	}

	return maxFileID
}

func (m *Manager) CurrentVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.currentVersion
}
