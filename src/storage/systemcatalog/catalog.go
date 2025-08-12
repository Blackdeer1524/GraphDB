package systemcatalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	catalogFile = "system_catalog.json"
)

var (
	ErrEntityNotFound = errors.New("entity not found")
	ErrEntityExists   = errors.New("entity already exists")
)

type Data struct {
	Metadata     Metadata               `json:"metadata"`
	VertexTables map[string]VertexTable `json:"vertex_tables"`
	EdgeTables   map[string]EdgeTable   `json:"edge_tables"`
	Indexes      map[string]Index       `json:"indexes"`
}

type Manager struct {
	basePath string

	mx *sync.RWMutex

	data *Data
}

func isFileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func New(basePath string) (*Manager, error) {
	path := filepath.Join(basePath, catalogFile)

	ok, err := isFileExists(path)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence of catalog file: %w", err)
	}

	sc := &Data{
		Metadata: Metadata{
			Version: "1.0",
			Name:    "graphdb",
		},
		VertexTables: make(map[string]VertexTable),
		EdgeTables:   make(map[string]EdgeTable),
		Indexes:      make(map[string]Index),
	}

	if !ok {
		data, err := json.Marshal(sc)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal to json: %w", err)
		}

		err = os.WriteFile(path, data, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to write catalog file: %w", err)
		}
	} else {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read catalog file: %w", err)
		}

		err = json.Unmarshal(data, sc)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal catalog file: %w", err)
		}
	}

	return &Manager{
		basePath: basePath,
		data:     sc,
		mx:       new(sync.RWMutex),
	}, nil
}

func (m *Manager) Save() error {
	m.mx.Lock()
	defer m.mx.Unlock()

	data, err := json.MarshalIndent(m.data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	path := filepath.Join(m.basePath, catalogFile)

	err = os.WriteFile(path, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write catalog file: %w", err)
	}

	return nil
}

func (m *Manager) GetVertexTableMeta(name string) (*VertexTable, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	table, exists := m.data.VertexTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return &table, nil
}

func (m *Manager) AddVertexTable(req VertexTable) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	_, exists := m.data.VertexTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.VertexTables[req.Name] = req

	return nil
}

func (m *Manager) VertexDropTable(name string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.VertexTables, name)

	return nil
}

func (m *Manager) VertexGetIndexes(name string) ([]Index, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	_, exists := m.data.VertexTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]Index, 0)

	for _, index := range m.data.Indexes {
		if index.TableName == name && index.TableKind == "vertex" {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) GetEdgeTableMeta(name string) (*EdgeTable, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	table, exists := m.data.EdgeTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return &table, nil
}

func (m *Manager) AddEdgeTable(req EdgeTable) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	_, exists := m.data.EdgeTables[req.Name]
	if exists {
		return ErrEntityExists
	}

	m.data.EdgeTables[req.Name] = req

	return nil
}

func (m *Manager) EdgeDropTable(name string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return ErrEntityNotFound
	}

	delete(m.data.EdgeTables, name)

	return nil
}

func (m *Manager) EdgeGetIndexes(name string) ([]Index, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	_, exists := m.data.EdgeTables[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	indexes := make([]Index, 0)

	for _, index := range m.data.Indexes {
		if index.TableName == name && index.TableKind == "edge" {
			indexes = append(indexes, index)
		}
	}

	return indexes, nil
}

func (m *Manager) GetIndex(name string) (*Index, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	index, exists := m.data.Indexes[name]
	if !exists {
		return nil, ErrEntityNotFound
	}

	return &index, nil
}

func (m *Manager) AddIndex(index Index) error {
	m.mx.Lock()
	defer m.mx.Unlock()

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
	m.mx.Lock()
	defer m.mx.Unlock()

	if _, exists := m.data.Indexes[name]; !exists {
		return ErrEntityNotFound
	}

	delete(m.data.Indexes, name)

	return nil
}

func (m *Manager) CopyData() *Data {
	m.mx.RLock()
	defer m.mx.RUnlock()

	metadata := Metadata{
		Version: m.data.Metadata.Version,
		Name:    m.data.Metadata.Name,
	}

	vertexTables := make(map[string]VertexTable, len(m.data.VertexTables))
	for name, table := range m.data.VertexTables {
		properties := make(map[string]Column, len(table.Schema))
		for k, v := range table.Schema {
			properties[k] = v
		}

		vertexTables[name] = VertexTable{
			Name:       table.Name,
			PathToFile: table.PathToFile,
			FileID:     table.FileID,
			Schema:     properties,
		}
	}

	edgeTables := make(map[string]EdgeTable, len(m.data.EdgeTables))
	for name, table := range m.data.EdgeTables {
		properties := make(map[string]Column, len(table.Schema))
		for k, v := range table.Schema {
			properties[k] = v
		}

		edgeTables[name] = EdgeTable{
			Name:       table.Name,
			PathToFile: table.PathToFile,
			FileID:     table.FileID,
			Schema:     properties,
		}
	}

	indexes := make(map[string]Index, len(m.data.Indexes))
	for name, index := range m.data.Indexes {
		columns := make([]string, len(index.Columns))
		copy(columns, index.Columns)

		indexes[name] = Index{
			Name:      index.Name,
			FileID:    index.FileID,
			TableName: index.TableName,
			Columns:   columns,
			TableKind: index.TableKind,
		}
	}

	return &Data{
		Metadata:     metadata,
		VertexTables: vertexTables,
		EdgeTables:   edgeTables,
		Indexes:      indexes,
	}
}
