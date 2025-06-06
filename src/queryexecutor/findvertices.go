package queryexecutor

import (
	"errors"
	"fmt"
	"io"

	"github.com/Blackdeer1524/GraphDB/src/storage/graph"
)

type FindRequest struct {
	TableName string
	TableKind graph.TableKind
	Filter    func(Row) bool
}

func findMatchingIndex(table graph.TableMetadata, filter FilterFunc) *graph.Index {
	return nil
}

func (qe *QueryExecutor) getRowSource(req FindRequest) (RowSource, error) {
	table, err := qe.Catalog.GetTable(req.TableName, req.TableKind)
	if err != nil {
		return nil, fmt.Errorf("get table %s: %w", req.TableName, err)
	}

	if index := findMatchingIndex(table, req.Filter); index != nil {
		return &IndexScanSource{
			indexFile:  index.File,
			keyColumns: nil,
		}, nil
	}

	return &TableScanSource{
		bufferPool: qe.BufferPool,
		table:      table,
		pageNum:    0,
		rowIndex:   0,
	}, nil
}

func collectRows(source RowSource, filter FilterFunc) ([]Row, error) {
	var result []Row

	for {
		row, err := source.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if filter == nil || filter(row) {
			result = append(result, row)
		}
	}

	return result, nil
}

func (qe *QueryExecutor) FindVertices(req FindRequest) ([]Row, error) {
	source, err := qe.getRowSource(req)
	if err != nil {
		return nil, err
	}

	return collectRows(source, req.Filter)
}
