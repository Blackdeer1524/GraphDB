package graph

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrNoSuchTable = errors.New("no such table")
)

type Graph struct {
	basePath    string
	catalog     *SystemCatalog
	catalogPath string
}

// CreateGraph creates a new empty graph
func CreateGraph(basePath string) (*Graph, error) {
	if _, err := os.Stat(basePath); err == nil {
		return nil, fmt.Errorf("graph already exists at %s", basePath)
	}

	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create graph directory: %w", err)
	}

	catalogPath := filepath.Join(basePath, "system_catalog.meta")

	cat := &SystemCatalog{}

	if err := SaveCatalog(cat, catalogPath); err != nil {
		return nil, fmt.Errorf("failed to save empty catalog: %w", err)
	}

	return &Graph{
		basePath:    basePath,
		catalog:     cat,
		catalogPath: catalogPath,
	}, nil
}

// LoadGraph loads an existing graph
func LoadGraph(basePath string) (*Graph, error) {
	catalogPath := filepath.Join(basePath, "system_catalog.meta")

	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("graph not found at %s", basePath)
	}

	cat, err := LoadCatalog(catalogPath)
	if err != nil {
		return nil, err
	}

	return &Graph{
		basePath:    basePath,
		catalog:     cat,
		catalogPath: catalogPath,
	}, nil
}

func (g *Graph) Save() error {
	return SaveCatalog(g.catalog, g.catalogPath)
}

func (g *Graph) CreateNodeTable(name string, schema []Column) error {
	_, err := CreateTable(g.catalog, g.basePath, name, VertexTable, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	err = g.Save()
	if err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

func (g *Graph) CreateEdgeTable(name string, schema []Column) error {
	_, err := CreateTable(g.catalog, g.basePath, name, EdgeTable, schema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	err = g.Save()
	if err != nil {
		return fmt.Errorf("failed to save catalog: %w", err)
	}

	return nil
}

func (g *Graph) ListTables() []TableMetadata {
	return g.catalog.Tables
}

func (g *Graph) GetTable(name string, kind TableKind) (TableMetadata, error) {
	for _, t := range g.catalog.Tables {
		if t.Name == name && t.Kind == kind {
			return t, nil
		}
	}

	return TableMetadata{}, ErrNoSuchTable
}
