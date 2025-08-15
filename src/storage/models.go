package storage

type Schema map[string]Column

type VertexTable struct {
	Name       string `json:"name"`
	PathToFile string `json:"path_to_file"`
	FileID     uint64 `json:"file_id"`
	Schema     Schema `json:"schema"`
}

type EdgeTable struct {
	Name       string `json:"name"`
	PathToFile string `json:"path_to_file"`
	FileID     uint64 `json:"file_id"`
	Schema     Schema `json:"schema"`
}

type Index struct {
	Name        string   `json:"name"`
	PathToFile  string   `json:"path_to_file"`
	FileID      uint64   `json:"id"`
	TableName   string   `json:"table_name"`
	TableKind   string   `json:"table_kind"`
	Columns     []string `json:"columns"`
	KeyBytesCnt uint32   `json:"key_bytes_cnt"`
}

type Column struct {
	Name string
	Type string
}

type Metadata struct {
	Version string `json:"version"`
	Name    string `json:"name"`
}
