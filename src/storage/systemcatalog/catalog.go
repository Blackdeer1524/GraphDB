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

func getDirIndexName(dirFileID common.FileID) string {
	return fmt.Sprintf("%d", dirFileID)
}

type Data struct {
	Metadata                storage.Metadata                       `json:"metadata"`
	VertexTables            map[string]storage.VertexTableMeta     `json:"vertex_tables"`
	DirTables               map[common.FileID]storage.DirTableMeta `json:"directory_tables"`
	EdgeTables              map[string]storage.EdgeTableMeta       `json:"edge_tables"`
	FileIDToVertexTableName map[common.FileID]string               `json:"file_id_to_vertex_table_name"`
	FileIDToEdgeTableName   map[common.FileID]string               `json:"file_id_to_edge_table_name"`
	VertexIndexes           map[string]storage.IndexMeta           `json:"vertex_indexes"`
	EdgeIndexes             map[string]storage.IndexMeta           `json:"edge_indexes"`
	DirIndexes              map[string]storage.IndexMeta           `json:"directory_indexes"`
}

func NewEmptyData() *Data {
	return &Data{
		Metadata:                storage.Metadata{},
		VertexTables:            map[string]storage.VertexTableMeta{},
		DirTables:               map[common.FileID]storage.DirTableMeta{},
		EdgeTables:              map[string]storage.EdgeTableMeta{},
		FileIDToVertexTableName: map[common.FileID]string{},
		FileIDToEdgeTableName:   map[common.FileID]string{},
		VertexIndexes:           map[string]storage.IndexMeta{},
		EdgeIndexes:             map[string]storage.IndexMeta{},
		DirIndexes:              map[string]storage.IndexMeta{},
	}
}

func (d *Data) Copy() Data {
	vertexTables := make(map[string]storage.VertexTableMeta)
	for k, v := range d.VertexTables {
		vertexTables[k] = v.Copy()
	}

	edgeTables := make(map[string]storage.EdgeTableMeta)
	for k, v := range d.EdgeTables {
		edgeTables[k] = v.Copy()
	}

	directoryTables := make(map[common.FileID]storage.DirTableMeta)
	for k, v := range d.DirTables {
		directoryTables[k] = v.Copy()
	}

	fileIDToVertexTableName := make(map[common.FileID]string)
	for k, v := range d.FileIDToVertexTableName {
		fileIDToVertexTableName[k] = v
	}

	fileIDToEdgeTableName := make(map[common.FileID]string)
	for k, v := range d.FileIDToEdgeTableName {
		fileIDToEdgeTableName[k] = v
	}

	vertexIndexes := make(map[string]storage.IndexMeta)
	for k, v := range d.VertexIndexes {
		vertexIndexes[k] = v.Copy()
	}

	edgeIndexes := make(map[string]storage.IndexMeta)
	for k, v := range d.EdgeIndexes {
		edgeIndexes[k] = v.Copy()
	}

	directoryIndexes := make(map[string]storage.IndexMeta)
	for k, v := range d.DirIndexes {
		directoryIndexes[k] = v.Copy()
	}

	return Data{
		Metadata:                d.Metadata.Copy(),
		VertexTables:            vertexTables,
		DirTables:               directoryTables,
		EdgeTables:              edgeTables,
		FileIDToVertexTableName: fileIDToVertexTableName,
		FileIDToEdgeTableName:   fileIDToEdgeTableName,
		VertexIndexes:           vertexIndexes,
		EdgeIndexes:             edgeIndexes,
		DirIndexes:              directoryIndexes,
	}
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
	isDirty        bool

	mu *sync.RWMutex
}

var _ storage.SystemCatalog = &Manager{}

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

	d := NewEmptyData()
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

func (m *Manager) Begin() error {
	m.mu.RLock()
	versionNum := utils.FromBytes[uint64](m.currentVersionPage.LockedRead(catalogVersionSlotNum))
	if m.currentVersion == versionNum && !m.isDirty {
		m.mu.RUnlock()
		return nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	versionNum = utils.FromBytes[uint64](m.currentVersionPage.LockedRead(catalogVersionSlotNum))
	if m.currentVersion == versionNum && !m.isDirty {
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
	m.isDirty = false

	return nil
}

func (m *Manager) GetBasePath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.basePath
}

func (m *Manager) CommitChanges(logger common.ITxnLoggerWithContext) (err error) {
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
			return loc, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to mark dirty: %w", err)
	}

	m.currentVersion++
	m.isDirty = false
	return nil
}

func (m *Manager) GetNewFileID() common.FileID {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.maxFileID++
	return common.FileID(m.maxFileID)
}

func (m *Manager) AddDirTable(req storage.DirTableMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.DirTables[req.VertexTableID]
	if exists {
		return ErrEntityExists
	}

	m.data.DirTables[req.VertexTableID] = req
	m.isDirty = true
	return nil
}

func (m *Manager) DirTableExists(vertexTableID common.FileID) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.DirTables[vertexTableID]
	return exists, nil
}

func (m *Manager) DropDirTable(vertexTableID common.FileID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.DirTables[vertexTableID]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.DirTables, vertexTableID)
	m.isDirty = true
	return nil
}

func (m *Manager) GetDirTableMeta(
	vertexTableID common.FileID,
) (storage.DirTableMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.data.DirTables[vertexTableID]
	if !exists {
		return storage.DirTableMeta{}, ErrEntityNotFound
	}

	return table, nil
}

func (m *Manager) GetEdgeTableNameByFileID(fileID common.FileID) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name, exists := m.data.FileIDToEdgeTableName[fileID]
	if !exists {
		return "", ErrEntityNotFound
	}

	return name, nil
}

func (m *Manager) GetVertexTableNameByFileID(fileID common.FileID) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name, exists := m.data.FileIDToVertexTableName[fileID]
	if !exists {
		return "", ErrEntityNotFound
	}

	return name, nil
}

func (m *Manager) GetVertexTableMeta(name string) (storage.VertexTableMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.data.VertexTables[name]
	if !exists {
		return storage.VertexTableMeta{}, ErrEntityNotFound
	}

	return table, nil
}

func (m *Manager) GetEdgeTableMeta(name string) (storage.EdgeTableMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	table, exists := m.data.EdgeTables[name]
	if !exists {
		return storage.EdgeTableMeta{}, ErrEntityNotFound
	}

	return table, nil
}

func (m *Manager) VertexTableExists(name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.VertexTables[name]

	return exists, nil
}

func (m *Manager) EdgeTableExists(name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.EdgeTables[name]

	return exists, nil
}

func (m *Manager) AddVertexTable(req storage.VertexTableMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.VertexTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.VertexTables[req.Name] = req
	m.isDirty = true
	return nil
}

func (m *Manager) AddEdgeTable(req storage.EdgeTableMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.EdgeTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.EdgeTables[req.Name] = req
	m.isDirty = true
	return nil
}

func (m *Manager) DropVertexTable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.VertexTables, name)
	m.isDirty = true
	return nil
}

func (m *Manager) DropEdgeTable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.EdgeTables, name)
	m.isDirty = true
	return nil
}

func (m *Manager) GetVertexTableIndexes(name string) ([]storage.IndexMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]storage.IndexMeta, 0)

	for _, index := range m.data.VertexIndexes {
		if index.TableName == name {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) GetEdgeTableIndexes(name string) ([]storage.IndexMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]storage.IndexMeta, 0)
	for _, index := range m.data.EdgeIndexes {
		if index.TableName == name {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) VertexIndexExists(name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.VertexIndexes[name]
	return exists, nil
}

func (m *Manager) EdgeIndexExists(name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.EdgeIndexes[name]
	return exists, nil
}

func (m *Manager) DirIndexExists(name string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data.DirIndexes[name]
	return exists, nil
}

func (m *Manager) GetVertexIndexMeta(name string) (storage.IndexMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.data.VertexIndexes[name]
	if !exists {
		return storage.IndexMeta{}, ErrEntityNotFound
	}

	return index, nil
}

func (m *Manager) GetEdgeIndexMeta(name string) (storage.IndexMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.data.EdgeIndexes[name]
	if !exists {
		return storage.IndexMeta{}, ErrEntityNotFound
	}

	return index, nil
}

func (m *Manager) GetDirIndexMeta(name string) (storage.IndexMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	index, exists := m.data.DirIndexes[name]
	if !exists {
		return storage.IndexMeta{}, ErrEntityNotFound
	}
	return index, nil
}

func (m *Manager) AddVertexIndex(index storage.IndexMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.VertexIndexes[index.Name]; exists {
		return ErrEntityExists
	}

	_, ok := m.data.VertexTables[index.TableName]
	if !ok {
		return fmt.Errorf("table %s not found", index.TableName)
	}

	m.data.VertexIndexes[index.Name] = index
	m.isDirty = true
	return nil
}

func (m *Manager) AddEdgeIndex(index storage.IndexMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.EdgeIndexes[index.Name]; exists {
		return ErrEntityExists
	}

	_, ok := m.data.EdgeTables[index.TableName]
	if !ok {
		return fmt.Errorf("table %s not found", index.TableName)
	}

	m.data.EdgeIndexes[index.Name] = index
	m.isDirty = true
	return nil
}

func (m *Manager) AddDirIndex(index storage.IndexMeta) error {
	assert.Assert(len(index.Columns) == 1, "directory index must have exactly 1 column")
	assert.Assert(index.Columns[0] == "ID", "directory index must have only `ID` column")

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.DirIndexes[index.Name]; exists {
		return ErrEntityExists
	}

	m.data.DirIndexes[index.Name] = index
	m.isDirty = true
	return nil
}

func (m *Manager) DropVertexIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.VertexIndexes[name]; !exists {
		return ErrEntityNotFound
	}

	delete(m.data.VertexIndexes, name)
	m.isDirty = true
	return nil
}

func (m *Manager) DropEdgeIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.EdgeIndexes[name]; !exists {
		return ErrEntityNotFound
	}

	delete(m.data.EdgeIndexes, name)
	m.isDirty = true
	return nil
}

func (m *Manager) DropDirIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data.DirIndexes[name]; !exists {
		return ErrEntityNotFound
	}

	delete(m.data.DirIndexes, name)
	m.isDirty = true
	return nil
}

func (m *Manager) CopyData() (Data, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.data.Copy(), nil
}

func (m *Manager) GetFileIDToPathMap() map[common.FileID]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	mp := make(map[common.FileID]string)

	for _, v := range m.data.VertexTables {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.EdgeTables {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.DirTables {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.VertexIndexes {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.EdgeIndexes {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	for _, v := range m.data.DirIndexes {
		mp[common.FileID(v.FileID)] = v.PathToFile
	}

	return mp
}

func calcMaxFileID(data *Data) uint64 {
	maxFileID := uint64(0)

	for _, v := range data.VertexTables {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	for _, v := range data.EdgeTables {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	for _, v := range data.DirTables {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	for _, v := range data.VertexIndexes {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	for _, v := range data.EdgeIndexes {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	for _, v := range data.DirIndexes {
		if uint64(v.FileID) > maxFileID {
			maxFileID = uint64(v.FileID)
		}
	}

	return maxFileID
}

func (m *Manager) CurrentVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.currentVersion
}

func GetDirTableName(vertexTableFileID common.FileID) string {
	return fmt.Sprintf("directory_%d", vertexTableFileID)
}
