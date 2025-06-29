package catalog

// TableKind represents the type of table
type TableKind string

const (
	VertexTable TableKind = "VERTEX"
	EdgeTable   TableKind = "EDGE"
	IndexTable  TableKind = "INDEX"
)

// DataType represents supported data types
type DataType string

const (
	TypeInt    DataType = "INT"
	TypeFloat  DataType = "FLOAT"
	TypeString DataType = "STRING"
	TypeBool   DataType = "BOOL"
	TypeDate   DataType = "DATE"
	TypeJSON   DataType = "JSON"
)

// Column represents a table column definition
type Column struct {
	Name         string   `json:"name"`
	Type         DataType `json:"type"`
	Nullable     bool     `json:"nullable"`
	DefaultValue *string  `json:"default_value,omitempty"`
	IsPrimary    bool     `json:"is_primary"`
	IsUnique     bool     `json:"is_unique"`
}

// IndexType represents the type of index
type IndexType string

const (
	IndexBTree IndexType = "BTREE"
)

// Index represents an index definition
type Index struct {
	Name      string    `json:"name"`
	Type      IndexType `json:"type"`
	TableName string    `json:"table_name"`
	Columns   []string  `json:"columns"`
	FilePath  string    `json:"file_path"`
	IsUnique  bool      `json:"is_unique"`
}

// TableMetadata represents table metadata
type TableMetadata struct {
	Name        string    `json:"name"`
	Kind        TableKind `json:"kind"`
	FilePath    string    `json:"file_path"`
	Schema      []Column  `json:"schema"`
	Description string    `json:"description,omitempty"`
}

// SystemCatalog represents the complete system catalog
type SystemCatalog struct {
	Tables   []TableMetadata `json:"tables"`
	Indexes  []Index         `json:"indexes"`
	Settings map[string]any  `json:"settings"`
}

// CreateTableRequest represents a request to create a table
type CreateTableRequest struct {
	Name        string    `json:"name"`
	Kind        TableKind `json:"kind"`
	Schema      []Column  `json:"schema"`
	Description string    `json:"description,omitempty"`
}

// CreateIndexRequest represents a request to create an index
type CreateIndexRequest struct {
	Name      string    `json:"name"`
	TableName string    `json:"table_name"`
	Type      IndexType `json:"type"`
	Columns   []string  `json:"columns"`
	IsUnique  bool      `json:"is_unique"`
}
