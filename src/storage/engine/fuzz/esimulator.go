package engine

// import (
// 	"os"
// 	"sync"
// 	"testing"
//
// 	"github.com/stretchr/testify/require"
//
// 	"github.com/Blackdeer1524/GraphDB/src/storage"
// )
//
// type engineSimulator struct {
// 	VertexTables  map[string]storage.Schema
// 	EdgeTables    map[string]storage.Schema
// 	VertexIndexes map[string]storage.IndexMeta
// 	EdgeIndexes   map[string]storage.IndexMeta
//
// 	mu *sync.RWMutex
// }
//
// func newEngineSimulator() *engineSimulator {
// 	return &engineSimulator{
// 		VertexTables:  make(map[string]storage.Schema),
// 		EdgeTables:    make(map[string]storage.Schema),
// 		VertexIndexes: make(map[string]storage.IndexMeta),
// 		EdgeIndexes:   make(map[string]storage.IndexMeta),
// 		mu:            new(sync.RWMutex),
// 	}
// }
//
// func (m *engineSimulator) apply(op Operation, res OpResult) {
// 	if !res.Success {
// 		return
// 	}
//
// 	m.mu.Lock()
// 	defer m.mu.Unlock()
//
// 	switch op.Type {
// 	case OpCreateVertexTable:
// 		if m.VertexTables == nil {
// 			m.VertexTables = make(map[string]storage.Schema)
// 		}
// 	case OpDropVertexTable:
// 		delete(m.VertexTables, op.Name)
// 		indexesToDelete := make([]string, 0)
// 		for idx, meta := range m.VertexIndexes {
// 			if meta.TableName == op.Name {
// 				indexesToDelete = append(indexesToDelete, idx)
// 			}
// 		}
// 		for _, idx := range indexesToDelete {
// 			delete(m.VertexIndexes, idx)
// 		}
// 	case OpCreateEdgeTable:
// 		if m.EdgeTables == nil {
// 			m.EdgeTables = make(map[string]storage.Schema)
// 		}
// 	case OpDropEdgeTable:
// 		delete(m.EdgeTables, op.Name)
// 		indexesToDelete := make([]string, 0)
// 		for idx, meta := range m.EdgeIndexes {
// 			if meta.TableName == op.Name {
// 				indexesToDelete = append(indexesToDelete, idx)
// 			}
// 		}
// 		for _, idx := range indexesToDelete {
// 			delete(m.EdgeIndexes, idx)
// 		}
// 	case OpCreateVertexIndex:
// 		m.VertexIndexes[op.Name] = storage.IndexMeta{
// 			TableName: op.Table,
// 			Columns:   append([]string(nil), op.Columns...),
// 		}
// 	case OpDropVertexIndex:
// 		delete(m.VertexIndexes, op.Name)
// 	case OpDropEdgeIndex:
// 		delete(m.EdgeIndexes, op.Name)
// 	default:
// 		panic("unhandled op type")
// 	}
// }
//
// func (m *engineSimulator) compareWithEngineFS(t *testing.T, baseDir string) {
// 	for tbl := range m.VertexTables {
// 		_, err := os.Stat(engine.getTableFilePath(baseDir, tbl))
// 		require.NoError(t, err, "table file is missing: %s", tbl)
// 	}
//
// 	for idx := range m.EdgeIndexes {
// 		_, err := os.Stat(engine.getTableFilePath(baseDir, idx))
// 		require.NoError(t, err, "index file is missing: %s", idx)
// 	}
// }
//
