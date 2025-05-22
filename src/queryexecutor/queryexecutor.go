package queryexecutor

import (
	"encoding/json"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/bufferpool"
	"github.com/Blackdeer1524/GraphDB/storage/graph"
	"github.com/Blackdeer1524/GraphDB/storage/page"
)

type QueryExecutor struct {
	Catalog    *graph.Graph
	BufferPool *bufferpool.Manager[*page.SlottedPage]
}

type Row map[string]any

type FilterFunc func(Row) bool

type RowSource interface {
	Next() (Row, error)
	//Reset() error
}

type TableScanSource struct {
	bufferPool *bufferpool.Manager[*page.SlottedPage]
	table      graph.TableMetadata

	pageNum  int
	rowIndex int
	current  []Row

	fileID uint64
}

func NewTableScanSource(bp *bufferpool.Manager[*page.SlottedPage], table graph.TableMetadata) RowSource {
	return &TableScanSource{
		bufferPool: bp,
		table:      table,
		pageNum:    0,
		rowIndex:   0,
	}
}

func (ts *TableScanSource) Next() (Row, error) {
	for {
		if ts.rowIndex < len(ts.current) {
			row := ts.current[ts.rowIndex]
			ts.rowIndex++

			return row, nil
		}

		pIdent := bufferpool.PageIdentity{
			FileID: ts.fileID,
			PageID: uint64(ts.pageNum),
		}

		page, err := ts.bufferPool.GetPage(pIdent)
		if err != nil {
			return nil, fmt.Errorf("cannot load page %d: %w", ts.pageNum, err)
		}

		ts.pageNum++

		rows := make([]Row, 0)

		numSlots := (*page).NumSlots()

		for i := uint32(0); i < numSlots; i++ {
			data, err := (*page).Get(i)
			if err != nil {
				return nil, fmt.Errorf("cannot read slot %d: %w", i, err)
			}

			var row Row
			if err := json.Unmarshal(data, &row); err != nil {
				return nil, fmt.Errorf("json decode failed: %w", err)
			}

			rows = append(rows, row)
		}

		ts.current = rows
		ts.rowIndex = 0
	}
}

func validateRowAgainstSchema(row Row, schema []graph.Column) error {
	for _, field := range schema {
		val, ok := row[field.Name]
		if !ok {
			return fmt.Errorf("missing field: %s", field.Name)
		}

		switch field.Type {
		case "string":
			if _, ok := val.(string); !ok {
				return fmt.Errorf("field %s must be a string", field.Name)
			}
		case "int":
			if _, ok := val.(int); !ok {
				return fmt.Errorf("field %s must be an int", field.Name)
			}
		case "bool":
			if _, ok := val.(bool); !ok {
				return fmt.Errorf("field %s must be a bool", field.Name)
			}
		default:
			return fmt.Errorf("unsupported type: %s", field.Type)
		}
	}

	return nil
}
