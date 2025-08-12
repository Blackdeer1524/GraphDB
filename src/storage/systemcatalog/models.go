package systemcatalog

type VertexTable struct {
	Name       string            `json:"name"`
	PathToFile string            `json:"path_to_file"`
	FileID     uint64            `json:"file_id"`
	Schema     map[string]Column `json:"schema"`
}

type EdgeTable struct {
	Name       string            `json:"name"`
	PathToFile string            `json:"path_to_file"`
	FileID     uint64            `json:"file_id"`
	Schema     map[string]Column `json:"schema"`
}

type Index struct {
	Name      string   `json:"name"`
	FileID    uint64   `json:"id"`
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns"`
	TableKind string   `json:"table_kind"`
}

type Column struct {
	Name string
	Type string
}

type Metadata struct {
	Version string `json:"version"`
	Name    string `json:"name"`
}
