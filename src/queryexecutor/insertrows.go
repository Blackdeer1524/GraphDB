package queryexecutor

import (
	"encoding/json"
	"fmt"

	"github.com/Blackdeer1524/GraphDB/bufferpool"
	"github.com/Blackdeer1524/GraphDB/storage/graph"
	"github.com/Blackdeer1524/GraphDB/storage/page"
)

func (qe *QueryExecutor) AppendRows(tableName string, tableKind graph.TableKind, rows []Row) error {
	table, err := qe.Catalog.GetTable(tableName, tableKind)
	if err != nil {
		return fmt.Errorf("failed to get table %s: %w", tableName, err)
	}

	for _, row := range rows {
		if err := validateRowAgainstSchema(row, table.Schema); err != nil {
			return fmt.Errorf("row validation failed: %w", err)
		}
	}

	pageNum := 0
	var currentPage *page.SlottedPage

	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row: %w", err)
		}

		for {
			pIdent := bufferpool.PageIdentity{
				PageID: uint64(pageNum),
			}

			currentPage, err = qe.BufferPool.GetPage(pIdent)
			if err != nil {
				return fmt.Errorf("failed to get page %d: %w", pageNum, err)
			}

			if _, err := currentPage.Insert(data); err == nil {
				break
			}

			pageNum++
		}
	}

	return nil
}
