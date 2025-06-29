package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrTableNotFound     = errors.New("table not found")
	ErrTableExists       = errors.New("table already exists")
	ErrIndexNotFound     = errors.New("index not found")
	ErrIndexExists       = errors.New("index already exists")
	ErrInvalidSchema     = errors.New("invalid schema")
	ErrInvalidTableName  = errors.New("invalid table name")
	ErrInvalidColumnName = errors.New("invalid column name")
)

const (
	CatalogFile = "system_catalog.json"
)

// Manager represents the system catalog manager
type Manager struct {
	basePath    string
	catalogPath string
	catalog     *SystemCatalog
}

// NewManager creates a new catalog manager
func NewManager(basePath string) *Manager {
	return &Manager{
		basePath:    basePath,
		catalogPath: filepath.Join(basePath, CatalogFile),
	}
}

// Initialize creates a new system catalog
func (m *Manager) Initialize() error {
	if _, err := os.Stat(m.catalogPath); err == nil {
		return fmt.Errorf("catalog already exists at %s", m.catalogPath)
	}

	m.catalog = &SystemCatalog{
		Tables:   []TableMetadata{},
		Indexes:  []Index{},
		Settings: make(map[string]any),
	}

	err := m.save()
	if err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

// Load loads an existing system catalog
func (m *Manager) Load() error {
	data, err := os.ReadFile(m.catalogPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("catalog not found at %s", m.catalogPath)
		}

		return fmt.Errorf("failed to read catalog: %w", err)
	}

	var catalog SystemCatalog

	err = json.Unmarshal(data, &catalog)
	if err != nil {
		return fmt.Errorf("invalid catalog format: %w", err)
	}

	m.catalog = &catalog

	return nil
}

// save saves the catalog to disk
func (m *Manager) save() error {
	if m.catalog == nil {
		return errors.New("catalog is not initialized")
	}

	data, err := json.MarshalIndent(m.catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize catalog: %w", err)
	}

	tmpPath := m.catalogPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp catalog file: %w", err)
	}

	err = os.Rename(tmpPath, m.catalogPath)
	if err != nil {
		return fmt.Errorf("failed to rename temp catalog file: %w", err)
	}

	return nil
}

// GetCatalog returns the current catalog
func (m *Manager) GetCatalog() *SystemCatalog {
	return m.catalog
}

// CreateTable creates a new table
func (m *Manager) CreateTable(req CreateTableRequest) (*TableMetadata, error) {
	if m.tableExists(req.Name, req.Kind) {
		return nil, ErrTableExists
	}

	if err := m.validateSchema(req.Schema); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	// Create directory structure
	subdir := m.getTableSubdir(req.Kind)
	dirPath := filepath.Join(m.basePath, subdir)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create %s directory: %w", subdir, err)
	}

	// Create table file
	filePath := filepath.Join(dirPath, req.Name+".tbl")

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table file %s: %w", filePath, err)
	}
	defer f.Close()

	table := TableMetadata{
		Name:        req.Name,
		Kind:        req.Kind,
		FilePath:    filePath,
		Schema:      req.Schema,
		Description: req.Description,
	}

	m.catalog.Tables = append(m.catalog.Tables, table)

	err = m.save()
	if err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}

	return &table, nil
}

// DropTable drops a table
func (m *Manager) DropTable(name string, kind TableKind) error {
	for i, table := range m.catalog.Tables {
		if table.Name == name && table.Kind == kind {
			if err := os.Remove(table.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("failed to remove table file: %w", err)
			}

			m.removeTableIndexes(name)

			m.catalog.Tables = append(m.catalog.Tables[:i], m.catalog.Tables[i+1:]...)

			err := m.save()
			if err != nil {
				return fmt.Errorf("failed to save catalog: %w", err)
			}

			return nil
		}
	}

	return ErrTableNotFound
}

// GetTable returns table metadata
func (m *Manager) GetTable(name string, kind TableKind) (*TableMetadata, error) {
	for _, table := range m.catalog.Tables {
		if table.Name == name && table.Kind == kind {
			return &table, nil
		}
	}

	return nil, ErrTableNotFound
}

// ListTables returns all tables
func (m *Manager) ListTables() []TableMetadata {
	return m.catalog.Tables
}

// ListTablesByKind returns tables of specific kind
func (m *Manager) ListTablesByKind(kind TableKind) []TableMetadata {
	tables := make([]TableMetadata, 0, len(m.catalog.Tables))

	for _, table := range m.catalog.Tables {
		if table.Kind == kind {
			tables = append(tables, table)
		}
	}

	return tables
}

// CreateIndex creates a new index
func (m *Manager) CreateIndex(req CreateIndexRequest) (*Index, error) {
	if m.indexExists(req.Name) {
		return nil, ErrIndexExists
	}

	table, err := m.GetTable(req.TableName, VertexTable)
	if err != nil {
		table, err = m.GetTable(req.TableName, EdgeTable)
		if err != nil {
			return nil, ErrTableNotFound
		}
	}

	if err := m.validateIndexColumns(table, req.Columns); err != nil {
		return nil, fmt.Errorf("invalid index columns: %w", err)
	}

	indexDir := filepath.Join(m.basePath, "indexes")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create indexes directory: %w", err)
	}

	indexPath := filepath.Join(indexDir, req.Name+".idx")
	f, err := os.Create(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}
	defer f.Close()

	index := Index{
		Name:      req.Name,
		Type:      req.Type,
		TableName: req.TableName,
		Columns:   req.Columns,
		FilePath:  indexPath,
		IsUnique:  req.IsUnique,
	}

	m.catalog.Indexes = append(m.catalog.Indexes, index)

	if err := m.save(); err != nil {
		return nil, fmt.Errorf("failed to save catalog: %w", err)
	}

	return &index, nil
}

// DropIndex drops an index
func (m *Manager) DropIndex(name string) error {
	for i, index := range m.catalog.Indexes {
		if index.Name == name {
			if err := os.Remove(index.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("failed to remove index file: %w", err)
			}

			m.catalog.Indexes = append(m.catalog.Indexes[:i], m.catalog.Indexes[i+1:]...)

			err := m.save()
			if err != nil {
				return fmt.Errorf("failed to save catalog: %w", err)
			}

			return nil
		}
	}

	return ErrIndexNotFound
}

// GetIndex returns index metadata
func (m *Manager) GetIndex(name string) (*Index, error) {
	for _, index := range m.catalog.Indexes {
		if index.Name == name {
			return &index, nil
		}
	}

	return nil, ErrIndexNotFound
}

// ListIndexes returns all indexes
func (m *Manager) ListIndexes() []Index {
	return m.catalog.Indexes
}

// ListIndexesByTable returns indexes for a specific table
func (m *Manager) ListIndexesByTable(tableName string) []Index {
	indexes := make([]Index, 0, len(m.catalog.Indexes))

	for _, index := range m.catalog.Indexes {
		if index.TableName == tableName {
			indexes = append(indexes, index)
		}
	}

	return indexes
}

// Helper methods

func (m *Manager) tableExists(name string, kind TableKind) bool {
	for _, table := range m.catalog.Tables {
		if table.Name == name && table.Kind == kind {
			return true
		}
	}

	return false
}

func (m *Manager) indexExists(name string) bool {
	for _, index := range m.catalog.Indexes {
		if index.Name == name {
			return true
		}
	}

	return false
}

func (m *Manager) getTableSubdir(kind TableKind) string {
	switch kind {
	case VertexTable:
		return "vertex"
	case EdgeTable:
		return "edge"
	default:
		return "tables"
	}
}

func (m *Manager) removeTableIndexes(tableName string) {
	remainingIndexes := make([]Index, 0, len(m.catalog.Indexes))
	for _, index := range m.catalog.Indexes {
		if index.TableName != tableName {
			remainingIndexes = append(remainingIndexes, index)
		} else {
			os.Remove(index.FilePath)
		}
	}

	m.catalog.Indexes = remainingIndexes
}

func (m *Manager) validateSchema(schema []Column) error {
	if len(schema) == 0 {
		return ErrInvalidSchema
	}

	columnNames := make(map[string]struct{})
	for _, col := range schema {
		if col.Name == "" {
			return ErrInvalidColumnName
		}

		if _, ok := columnNames[col.Name]; ok {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}

		columnNames[col.Name] = struct{}{}
	}

	return nil
}

func (m *Manager) validateIndexColumns(table *TableMetadata, columns []string) error {
	if len(columns) == 0 {
		return ErrInvalidSchema
	}

	tableColumns := make(map[string]struct{})
	for _, col := range table.Schema {
		tableColumns[col.Name] = struct{}{}
	}

	for _, colName := range columns {
		if _, ok := tableColumns[colName]; !ok {
			return fmt.Errorf("column %s not found in table %s", colName, table.Name)
		}
	}

	return nil
}
