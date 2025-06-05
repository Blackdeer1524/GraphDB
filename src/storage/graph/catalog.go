package graph

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type TableKind string

const (
	VertexTable TableKind = "VERTEX"
	EdgeTable   TableKind = "EDGE"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Index struct {
	File        string `json:"string"`
	KeyColumns  string `json:"key_columns"`
	DataColumns string `json:"data_columns"`
}

type TableMetadata struct {
	Name     string    `json:"name"`
	Kind     TableKind `json:"kind"`
	FilePath string    `json:"file_path"`
	Schema   []Column  `json:"schema"`
	Indexes  []Index   `json:"indexes"`
}

type SystemCatalog struct {
	Tables []TableMetadata `json:"tables"`
}

func LoadCatalog(path string) (*SystemCatalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &SystemCatalog{}, nil
		}

		return nil, fmt.Errorf("failed to read catalog: %w", err)
	}

	var catalog SystemCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("invalid catalog format: %w", err)
	}

	return &catalog, nil
}

func SaveCatalog(catalog *SystemCatalog, path string) error {
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize catalog: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp catalog file: %w", err)
	}

	return os.Rename(tmpPath, path)
}

func tableExists(catalog *SystemCatalog, name string, kind TableKind) bool {
	for _, t := range catalog.Tables {
		if t.Name == name && t.Kind == kind {
			return true
		}
	}

	return false
}

func GetTableFile(cat *SystemCatalog, name string, kind TableKind) (string, error) {
	for _, t := range cat.Tables {
		if t.Name == name && t.Kind == kind {
			return t.FilePath, nil
		}
	}
	return "", fmt.Errorf("table %s (%s) not found", name, kind)
}

func CreateTable(
	cat *SystemCatalog,
	basePath string,
	name string,
	kind TableKind,
	schema []Column,
) (*TableMetadata, error) {
	if tableExists(cat, name, kind) {
		return nil, fmt.Errorf("table %s (%s) already exists", name, kind)
	}

	var subdir string
	switch kind {
	case VertexTable:
		subdir = "vertex"
	case EdgeTable:
		subdir = "edge"
	default:
		return nil, fmt.Errorf("unknown table kind: %s", kind)
	}

	dirPath := filepath.Join(basePath, subdir)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create %s directory: %w", subdir, err)
	}

	filePath := filepath.Join(dirPath, name+".tbl")

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table file %s: %w", filePath, err)
	}
	defer f.Close()

	meta := TableMetadata{
		Name:     name,
		Kind:     kind,
		Schema:   schema,
		FilePath: filePath,
	}

	cat.Tables = append(cat.Tables, meta)

	return &meta, nil
}
