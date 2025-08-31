package storage

type ColumnType string

const (
	ColumnTypeInt64   ColumnType = "int64"
	ColumnTypeUint64  ColumnType = "uint64"
	ColumnTypeFloat64 ColumnType = "float64"
	ColumnTypeUUID    ColumnType = "uuid" // 16 bytes
)

type Column struct {
	Name string
	Type ColumnType
}

type Schema []Column

type TableMeta struct {
	Name       string `json:"name"`
	PathToFile string `json:"path_to_file"`
	FileID     uint64 `json:"file_id"`
	Schema     Schema `json:"schema"`
}

func (v *TableMeta) Copy() TableMeta {
	schemaCopy := make(Schema, len(v.Schema))
	copy(schemaCopy, v.Schema)

	return TableMeta{
		Name:       v.Name,
		PathToFile: v.PathToFile,
		FileID:     v.FileID,
		Schema:     schemaCopy,
	}
}

type IndexMeta struct {
	Name        string   `json:"name"`
	PathToFile  string   `json:"path_to_file"`
	FileID      uint64   `json:"id"`
	TableName   string   `json:"table_name"`
	Columns     []string `json:"columns"`
	KeyBytesCnt uint32   `json:"key_bytes_cnt"`
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
