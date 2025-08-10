package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type VertexTable struct {
	ID         uint64            `json:"id"`
	Name       string            `json:"name"`
	PathToFile string            `json:"path_to_file"`
	Schema     map[string]Column `json:"schema"`
}

type NodeTable struct {
	ID         uint64            `json:"id"`
	Name       string            `json:"name"`
	PathToFile string            `json:"path_to_file"`
	Schema     map[string]Column `json:"schema"`
}

type Index struct {
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
}

type CreateIndexRequest struct {
}

type Column struct {
	Name string
	Type string
}

type SystemCatalog struct {
	Metadata     map[string]any         `json:"metadata"`
	VertexTables map[string]VertexTable `json:"vertex_tables"`
	NodeTables   map[string]NodeTable   `json:"nodes_tables"`
	Indexes      map[string]Index       `json:"indexes"`
}

// save needs lock before calling
func (m *Manager) save() error {
	if m.catalog == nil {
		return errors.New("catalog is not initialized")
	}

	data, err := json.MarshalIndent(m.catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize catalog: %w", err)
	}

	tmpPath := m.catalogPath + ".tmp"

	err = os.WriteFile(tmpPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write temp catalog file: %w", err)
	}

	err = os.Rename(tmpPath, m.catalogPath)
	if err != nil {
		return fmt.Errorf("failed to rename temp catalog file: %w", err)
	}

	return nil
}
