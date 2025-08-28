package fuzz

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/engine"
)

type engineSimulator struct {
	VertexTables map[string]storage.Schema
	EdgeTables   map[string]storage.Schema
	Indexes      map[string]storage.Index

	mu *sync.RWMutex
}

func newEngineSimulator() *engineSimulator {
	return &engineSimulator{
		VertexTables: make(map[string]storage.Schema),
		EdgeTables:   make(map[string]storage.Schema),
		Indexes:      make(map[string]storage.Index),

		mu: new(sync.RWMutex),
	}
}

func (m *engineSimulator) apply(op Operation, res OpResult) {
	if !res.Success {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	switch op.Type {
	case OpCreateVertexTable:
		if m.VertexTables == nil {
			m.VertexTables = make(map[string]storage.Schema)
		}
	case OpDropVertexTable:
		delete(m.VertexTables, op.Name)

		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name && meta.TableKind == "vertex" {
				delete(m.Indexes, idx)
			}
		}
	case OpCreateEdgeTable:
		if m.EdgeTables == nil {
			m.EdgeTables = make(map[string]storage.Schema)
		}
	case OpDropEdgeTable:
		delete(m.EdgeTables, op.Name)
		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name && meta.TableKind == "edge" {
				delete(m.Indexes, idx)
			}
		}
	case OpCreateIndex:
		m.Indexes[op.Name] = storage.Index{
			TableName: op.Table,
			TableKind: op.TableKind,
			Columns:   append([]string(nil), op.Columns...),
		}
	case OpDropIndex:
		delete(m.Indexes, op.Name)
	default:
		panic("unhandled op type")
	}
}

func (m *engineSimulator) compareWithEngineFS(
	t *testing.T,
	baseDir string,
	se *engine.StorageEngine,
	l common.ITxnLoggerWithContext,
) {
	for tbl := range m.VertexTables {
		_, err := os.Stat(engine.GetVertexTableFilePath(baseDir, tbl))
		require.NoError(t, err, "vertex table file is missing: %s", tbl)
	}

	for tbl := range m.EdgeTables {
		_, err := os.Stat(engine.GetEdgeTableFilePath(baseDir, tbl))
		require.NoError(t, err, "edge table file is missing: %s", tbl)
	}

	for idx := range m.Indexes {
		_, err := os.Stat(engine.GetIndexFilePath(baseDir, idx))
		require.NoError(t, err, "index file is missing: %s", idx)
	}

	for tbl, sch := range m.VertexTables {
		err := se.CreateVertexTable(0, tbl, sch, l)
		require.Error(t, err, "expected error on duplicate CreateVertexTable(%s)", tbl)
	}

	for tbl, sch := range m.EdgeTables {
		err := se.CreateEdgesTable(0, tbl, sch, l)
		require.Error(t, err, "expected error on duplicate CreateEdgesTable(%s)", tbl)
	}

	for idx, meta := range m.Indexes {
		err := se.CreateIndex(
			0,
			idx,
			meta.TableName,
			meta.TableKind,
			meta.Columns,
			8,
			l,
		)
		require.Error(t, err, "expected error on duplicate CreateIndex(%s)", idx)
	}
}
