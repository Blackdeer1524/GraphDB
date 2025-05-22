package queryexecutor

import (
	"errors"
	"fmt"
	"io"

	"github.com/Blackdeer1524/GraphDB/storage/graph"
)

type FindRequest struct {
	TableName string
	Filter    func(Row) bool
}

func (qe *QueryExecutor) getRowSource(req FindRequest, kind graph.TableKind) (RowSource, error) {
	table, err := qe.Catalog.GetTable(req.TableName, kind)
	if err != nil {
		return nil, fmt.Errorf("get table %s: %w", req.TableName, err)
	}

	return NewTableScanSource(qe.BufferPool, table), nil
}

func (qe *QueryExecutor) FindVertices(req FindRequest) ([]Row, error) {
	source, err := qe.getRowSource(req, graph.VertexTable)
	if err != nil {
		return nil, err
	}

	var result []Row
	for {
		row, err := source.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if req.Filter == nil || req.Filter(row) {
			result = append(result, row)
		}
	}

	return result, nil
}
