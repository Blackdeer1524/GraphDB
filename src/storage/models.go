package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

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

type Schema []Column

type VertexTableMeta struct {
	Name       string        `json:"name"`
	FileID     common.FileID `json:"file_id"`
	PathToFile string        `json:"path_to_file"`
	Schema     Schema        `json:"schema"`
}

func (v *VertexTableMeta) Copy() VertexTableMeta {
	schemaCopy := make(Schema, len(v.Schema))
	copy(schemaCopy, v.Schema)

	return VertexTableMeta{
		Name:       v.Name,
		FileID:     v.FileID,
		PathToFile: v.PathToFile,
		Schema:     schemaCopy,
	}
}

type EdgeTableMeta struct {
	Name            string        `json:"name"`
	FileID          common.FileID `json:"file_id"`
	PathToFile      string        `json:"path_to_file"`
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
		PathToFile:      v.PathToFile,
		Schema:          schemaCopy,
		SrcVertexFileID: v.SrcVertexFileID,
		DstVertexFileID: v.DstVertexFileID,
	}
}

type DirTableMeta struct {
	VertexTableID common.FileID `json:"vertex_table_id"`
	FileID        common.FileID `json:"file_id"`
	PathToFile    string        `json:"path_to_file"`
}

func (d *DirTableMeta) Copy() DirTableMeta {
	return DirTableMeta{
		VertexTableID: d.VertexTableID,
		FileID:        d.FileID,
		PathToFile:    d.PathToFile,
	}
}

type IndexMeta struct {
	Name        string        `json:"name"`
	PathToFile  string        `json:"path_to_file"`
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
		PathToFile:  i.PathToFile,
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

type DirectoryItemInternalFields struct {
	ID         DirItemInternalID
	NextItemID DirItemInternalID
	PrevItemID DirItemInternalID
}

func NewDirectoryItemInternalFields(
	ID DirItemInternalID,
	NextItemID DirItemInternalID,
	PrevItemID DirItemInternalID,
) DirectoryItemInternalFields {
	return DirectoryItemInternalFields{
		ID:         ID,
		NextItemID: NextItemID,
		PrevItemID: PrevItemID,
	}
}

type DirectoryItemGraphFields struct {
	VertexID   VertexInternalID
	EdgeFileID common.FileID
	EdgeID     EdgeInternalID
}

func NewDirectoryItemGraphFields(
	VertexID VertexInternalID,
	EdgeFileID common.FileID,
	EdgeID EdgeInternalID,
) DirectoryItemGraphFields {
	return DirectoryItemGraphFields{
		VertexID:   VertexID,
		EdgeFileID: EdgeFileID,
		EdgeID:     EdgeID,
	}
}

type DirectoryItem struct {
	DirectoryItemInternalFields
	DirectoryItemGraphFields
}

type EdgeInternalFields struct {
	ID              EdgeInternalID
	DirectoryItemID DirItemInternalID
	SrcVertexID     VertexInternalID
	DstVertexID     VertexInternalID
	NextEdgeID      EdgeInternalID
	PrevEdgeID      EdgeInternalID
}

func NewEdgeInternalFields(
	ID EdgeInternalID,
	DirectoryItemID DirItemInternalID,
	SrcVertexID VertexInternalID,
	DstVertexID VertexInternalID,
	PrevEdgeID EdgeInternalID,
	NextEdgeID EdgeInternalID,
) EdgeInternalFields {
	return EdgeInternalFields{
		ID:              ID,
		DirectoryItemID: DirectoryItemID,
		SrcVertexID:     SrcVertexID,
		DstVertexID:     DstVertexID,
		NextEdgeID:      NextEdgeID,
		PrevEdgeID:      PrevEdgeID,
	}
}

type VertexInternalFields struct {
	ID        VertexInternalID
	DirItemID DirItemInternalID
}

func NewVertexInternalFields(
	ID VertexInternalID,
	DirItemID DirItemInternalID,
) VertexInternalFields {
	return VertexInternalFields{
		ID:        ID,
		DirItemID: DirItemID,
	}
}
