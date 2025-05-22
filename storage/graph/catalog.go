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
	NodeTable TableKind = "NODE"
	EdgeTable TableKind = "EDGE"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type TableMetadata struct {
	Name     string    `json:"name"`
	Kind     TableKind `json:"kind"`
	FilePath string    `json:"file_path"`
	Schema   []Column  `json:"schema"`
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

func GetTableFile(catalog *SystemCatalog, name string, kind TableKind) (string, error) {
	for _, t := range catalog.Tables {
		if t.Name == name && t.Kind == kind {
			return t.FilePath, nil
		}
	}

	return "", fmt.Errorf("table %s (%s) not found", name, kind)
}

func CreateTable(
	catalog *SystemCatalog,
	basePath string,
	name string,
	kind TableKind,
	schema []Column,
) (*TableMetadata, error) {
	if tableExists(catalog, name, kind) {
		return nil, fmt.Errorf("table %s (%s) already exists", name, kind)
	}

	fileName := fmt.Sprintf("%s_%s.tbl", kind, name)
	filePath := filepath.Join(basePath, fileName)

	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create table file: %w", err)
	}
	defer f.Close()

	metadata := TableMetadata{
		Name:     name,
		Kind:     kind,
		FilePath: filePath,
		Schema:   schema,
	}

	catalog.Tables = append(catalog.Tables, metadata)

	return &metadata, nil
}
