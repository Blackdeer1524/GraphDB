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
	Tables  map[string]storage.Schema
	Indexes map[string]storage.IndexMeta

	mu *sync.RWMutex
}

func newEngineSimulator() *engineSimulator {
	return &engineSimulator{
		Tables:  make(map[string]storage.Schema),
		Indexes: make(map[string]storage.IndexMeta),
		mu:      new(sync.RWMutex),
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
		if m.Tables == nil {
			m.Tables = make(map[string]storage.Schema)
		}
	case OpDropVertexTable:
		delete(m.Tables, op.Name)
		indexesToDelete := make([]string, 0)
		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name {
				indexesToDelete = append(indexesToDelete, idx)
			}
		}
		for _, idx := range indexesToDelete {
			delete(m.Indexes, idx)
		}
	case OpCreateEdgeTable:
		if m.Tables == nil {
			m.Tables = make(map[string]storage.Schema)
		}
	case OpDropEdgeTable:
		delete(m.Tables, op.Name)
		indexesToDelete := make([]string, 0)
		for idx, meta := range m.Indexes {
			if meta.TableName == op.Name {
				indexesToDelete = append(indexesToDelete, idx)
			}
		}
		for _, idx := range indexesToDelete {
			delete(m.Indexes, idx)
		}
	case OpCreateIndex:
		m.Indexes[op.Name] = storage.IndexMeta{
			TableName: op.Table,
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
	se *storage.StorageEngine,
	l common.ITxnLoggerWithContext,
) {
	for tbl := range m.Tables {
		_, err := os.Stat(engine.GetTableFilePath(baseDir, tbl))
		require.NoError(t, err, "table file is missing: %s", tbl)
	}

	for idx := range m.Indexes {
		_, err := os.Stat(engine.GetIndexFilePath(baseDir, idx))
		require.NoError(t, err, "index file is missing: %s", idx)
	}
}
