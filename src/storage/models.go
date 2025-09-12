package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/google/uuid"

	"github.com/Blackdeer1524/GraphDB/src/pkg/assert"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

type ColumnType string

const (
	ColumnTypeInt64   ColumnType = "int64"
	ColumnTypeUint64  ColumnType = "uint64"
	ColumnTypeFloat64 ColumnType = "float64"
	ColumnTypeUUID    ColumnType = "uuid" // 16 bytes
)

func ColumnToFloat(left any) (float64, error) {
	switch left := left.(type) {
	case int64:
		return float64(left), nil
	case uint64:
		return float64(left), nil
	case float64:
		return left, nil
	case uuid.UUID:
		return 0, fmt.Errorf("can't cast a column of type UUID to float64: %v", left)
	}
	panic("unsupported column type: " + fmt.Sprintf("%#v", left))
}

func CmpColumnValue(left any, right []byte) bool {
	switch left := left.(type) {
	case int64:
		var result int64
		err := binary.Read(bytes.NewReader(right), binary.BigEndian, &result)
		assert.NoError(err)
		return left == result
	case uint64:
		var result uint64
		err := binary.Read(bytes.NewReader(right), binary.BigEndian, &result)
		assert.NoError(err)
		return left == result
	case float64:
		var result float64
		err := binary.Read(bytes.NewReader(right), binary.BigEndian, &result)
		assert.NoError(err)
		return left == result
	case uuid.UUID:
		var result uuid.UUID
		err := binary.Read(bytes.NewReader(right), binary.BigEndian, &result)
		assert.NoError(err)
		return left == result
	}
	panic("unsupported column type: " + fmt.Sprintf("%#v", left))
}

type Column struct {
	Name string
	Type ColumnType
}

func (c ColumnType) Size() int {
	switch c {
	case ColumnTypeInt64:
		return int(unsafe.Sizeof(int64(0)))
	case ColumnTypeUint64:
		return int(unsafe.Sizeof(uint64(0)))
	case ColumnTypeFloat64:
		return int(unsafe.Sizeof(float64(0)))
	case ColumnTypeUUID:
		return int(unsafe.Sizeof(uuid.UUID{}))
	}
	panic("unsupported column type: " + fmt.Sprintf("%#v", c))
}

type Schema []Column

type VertexTableMeta struct {
	Name   string        `json:"name"`
	FileID common.FileID `json:"file_id"`
	Schema Schema        `json:"schema"`
}

func (v *VertexTableMeta) Copy() VertexTableMeta {
	schemaCopy := make(Schema, len(v.Schema))
	copy(schemaCopy, v.Schema)

	return VertexTableMeta{
		Name:   v.Name,
		FileID: v.FileID,
		Schema: schemaCopy,
	}
}

type EdgeTableMeta struct {
	Name            string        `json:"name"`
	FileID          common.FileID `json:"file_id"`
	Schema          Schema        `json:"schema"`
	SrcVertexFileID common.FileID `json:"src_vertex_file_id"`
	DstVertexFileID common.FileID `json:"dst_vertex_file_id"`
}

func (v *EdgeTableMeta) Copy() EdgeTableMeta {
	schemaCopy := make(Schema, len(v.Schema))
	copy(schemaCopy, v.Schema)

	return EdgeTableMeta{
		Name:            v.Name,
		FileID:          v.FileID,
		Schema:          schemaCopy,
		SrcVertexFileID: v.SrcVertexFileID,
		DstVertexFileID: v.DstVertexFileID,
	}
}

type DirTableMeta struct {
	VertexTableID common.FileID `json:"vertex_table_id"`
	FileID        common.FileID `json:"file_id"`
}

func (d *DirTableMeta) Copy() DirTableMeta {
	return DirTableMeta{
		VertexTableID: d.VertexTableID,
		FileID:        d.FileID,
	}
}

type IndexMeta struct {
	Name        string        `json:"name"`
	FileID      common.FileID `json:"id"`
	TableName   string        `json:"table_name"`
	Columns     []string      `json:"columns"`
	KeyBytesCnt uint32        `json:"key_bytes_cnt"`
}

func (i *IndexMeta) Copy() IndexMeta {
	columnsCopy := make([]string, len(i.Columns))
	copy(columnsCopy, i.Columns)

	return IndexMeta{
		Name:        i.Name,
		FileID:      i.FileID,
		TableName:   i.TableName,
		Columns:     columnsCopy,
		KeyBytesCnt: i.KeyBytesCnt,
	}
}

type Metadata struct {
	Version string `json:"version"`
	Name    string `json:"name"`
}

func (m *Metadata) Copy() Metadata {
	return Metadata{
		Version: m.Version,
		Name:    m.Name,
	}
}

type DirectoryItemSystemFields struct {
	ID         DirItemSystemID
	NextItemID DirItemSystemID
	PrevItemID DirItemSystemID
}

func NewDirectoryItemSystemFields(
	ID DirItemSystemID,
	NextItemID DirItemSystemID,
	PrevItemID DirItemSystemID,
) DirectoryItemSystemFields {
	return DirectoryItemSystemFields{
		ID:         ID,
		NextItemID: NextItemID,
		PrevItemID: PrevItemID,
	}
}

type DirectoryItemGraphFields struct {
	VertexID   VertexSystemID
	EdgeFileID common.FileID
	EdgeID     EdgeSystemID
}

func NewDirectoryItemGraphFields(
	VertexID VertexSystemID,
	EdgeFileID common.FileID,
	EdgeID EdgeSystemID,
) DirectoryItemGraphFields {
	return DirectoryItemGraphFields{
		VertexID:   VertexID,
		EdgeFileID: EdgeFileID,
		EdgeID:     EdgeID,
	}
}

type DirectoryItem struct {
	DirectoryItemSystemFields
	DirectoryItemGraphFields
}

func GetDirectoryItemSchemaMap() map[string]ColumnType {
	DirectoryItemSchemaMap := map[string]ColumnType{
		"ID":           ColumnTypeUUID,
		"NEXT_ITEM_ID": ColumnTypeUUID,
		"PREV_ITEM_ID": ColumnTypeUUID,
		"VERTEX_ID":    ColumnTypeUUID,
		"EDGE_FILE_ID": ColumnTypeUint64,
		"EDGE_ID":      ColumnTypeUUID,
	}
	return DirectoryItemSchemaMap
}

type EdgeSystemFields struct {
	ID              EdgeSystemID
	DirectoryItemID DirItemSystemID
	SrcVertexID     VertexSystemID
	DstVertexID     VertexSystemID
	NextEdgeID      EdgeSystemID
	PrevEdgeID      EdgeSystemID
}

func GetEdgeSystemSchemaMap() map[string]ColumnType {
	EdgeSchemaMap := map[string]ColumnType{
		"ID":            ColumnTypeUUID,
		"DIR_ITEM_ID":   ColumnTypeUUID,
		"SRC_VERTEX_ID": ColumnTypeUUID,
		"DST_VERTEX_ID": ColumnTypeUUID,
		"NEXT_EDGE_ID":  ColumnTypeUUID,
		"PREV_EDGE_ID":  ColumnTypeUUID,
	}
	return EdgeSchemaMap
}

func NewEdgeSystemFields(
	ID EdgeSystemID,
	DirectoryItemID DirItemSystemID,
	SrcVertexID VertexSystemID,
	DstVertexID VertexSystemID,
	PrevEdgeID EdgeSystemID,
	NextEdgeID EdgeSystemID,
) EdgeSystemFields {
	return EdgeSystemFields{
		ID:              ID,
		DirectoryItemID: DirectoryItemID,
		SrcVertexID:     SrcVertexID,
		DstVertexID:     DstVertexID,
		NextEdgeID:      NextEdgeID,
		PrevEdgeID:      PrevEdgeID,
	}
}

type VertexSystemFields struct {
	ID        VertexSystemID
	DirItemID DirItemSystemID
}

func GetVertexSystemSchemaMap() map[string]ColumnType {
	VertexSchemaMap := map[string]ColumnType{
		"ID":          ColumnTypeUUID,
		"DIR_ITEM_ID": ColumnTypeUUID,
	}
	return VertexSchemaMap
}

func NewVertexSystemFields(
	ID VertexSystemID,
	DirItemID DirItemSystemID,
) VertexSystemFields {
	return VertexSystemFields{
		ID:        ID,
		DirItemID: DirItemID,
	}
}

type EdgeInfo struct {
	SystemID    EdgeSystemID
	SrcVertexID VertexSystemID
	DstVertexID VertexSystemID
	Data        map[string]any
}

type VertexInfo struct {
	SystemID VertexSystemID
	Data     map[string]any
}
