package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManager_Initialize(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	// Test initialization
	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	// Check if catalog file was created
	catalogPath := filepath.Join(tempDir, CatalogFile)
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		t.Fatal("Catalog file was not created")
	}

	// Check catalog structure
	catalog := manager.GetCatalog()
	if catalog == nil {
		t.Fatal("Catalog is nil after initialization")
	}

	if len(catalog.Tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(catalog.Tables))
	}

	if len(catalog.Indexes) != 0 {
		t.Errorf("Expected 0 indexes, got %d", len(catalog.Indexes))
	}

	// Test double initialization
	err = manager.Initialize()
	if err == nil {
		t.Fatal("Expected error when initializing existing catalog")
	}
}

func TestManager_Load(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	// Test loading non-existent catalog
	err := manager.Load()
	if err == nil {
		t.Fatal("Expected error when loading non-existent catalog")
	}

	// Initialize catalog first
	err = manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	// Create new manager and load
	newManager := NewManager(tempDir)
	err = newManager.Load()
	if err != nil {
		t.Fatalf("Failed to load catalog: %v", err)
	}

	catalog := newManager.GetCatalog()
	if catalog == nil {
		t.Fatal("Catalog is nil after loading")
	}
}

func TestManager_CreateTable(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: false},
		{Name: "age", Type: TypeInt, Nullable: true},
	}

	req := CreateTableRequest{
		Name:        "users",
		Kind:        VertexTable,
		Schema:      schema,
		Description: "User vertices",
	}

	// Test creating table
	table, err := manager.CreateTable(req)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got %s", table.Name)
	}

	if table.Kind != VertexTable {
		t.Errorf("Expected table kind %s, got %s", VertexTable, table.Kind)
	}

	if len(table.Schema) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(table.Schema))
	}

	// Check if table file was created
	if _, err := os.Stat(table.FilePath); os.IsNotExist(err) {
		t.Fatal("Table file was not created")
	}

	// Test creating duplicate table
	_, err = manager.CreateTable(req)
	if err != ErrTableExists {
		t.Errorf("Expected ErrTableExists, got %v", err)
	}

	// Test creating table with invalid schema
	invalidSchemaReq := CreateTableRequest{
		Name:   "invalid",
		Kind:   VertexTable,
		Schema: []Column{},
	}
	_, err = manager.CreateTable(invalidSchemaReq)
	if err != ErrInvalidSchema {
		t.Errorf("Expected ErrInvalidSchema, got %v", err)
	}
}

func TestManager_DropTable(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString},
	}

	req := CreateTableRequest{
		Name:   "test_table",
		Kind:   VertexTable,
		Schema: schema,
	}

	table, err := manager.CreateTable(req)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test dropping table
	err = manager.DropTable("test_table", VertexTable)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Check if table file was removed
	if _, err := os.Stat(table.FilePath); !os.IsNotExist(err) {
		t.Fatal("Table file was not removed")
	}

	// Test dropping non-existent table
	err = manager.DropTable("non_existent", VertexTable)
	if err != ErrTableNotFound {
		t.Errorf("Expected ErrTableNotFound, got %v", err)
	}
}

func TestManager_GetTable(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString},
	}

	req := CreateTableRequest{
		Name:   "test_table",
		Kind:   VertexTable,
		Schema: schema,
	}

	_, err = manager.CreateTable(req)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test getting existing table
	table, err := manager.GetTable("test_table", VertexTable)
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if table.Name != "test_table" {
		t.Errorf("Expected table name 'test_table', got %s", table.Name)
	}

	// Test getting non-existent table
	_, err = manager.GetTable("non_existent", VertexTable)
	if err != ErrTableNotFound {
		t.Errorf("Expected ErrTableNotFound, got %v", err)
	}
}

func TestManager_ListTables(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString},
	}

	// Create vertex table
	vertexReq := CreateTableRequest{
		Name:   "users",
		Kind:   VertexTable,
		Schema: schema,
	}
	_, err = manager.CreateTable(vertexReq)
	if err != nil {
		t.Fatalf("Failed to create vertex table: %v", err)
	}

	// Create edge table
	edgeReq := CreateTableRequest{
		Name:   "friendships",
		Kind:   EdgeTable,
		Schema: schema,
	}
	_, err = manager.CreateTable(edgeReq)
	if err != nil {
		t.Fatalf("Failed to create edge table: %v", err)
	}

	// Test listing all tables
	tables := manager.ListTables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}

	// Test listing vertex tables
	vertexTables := manager.ListTablesByKind(VertexTable)
	if len(vertexTables) != 1 {
		t.Errorf("Expected 1 vertex table, got %d", len(vertexTables))
	}

	if vertexTables[0].Name != "users" {
		t.Errorf("Expected vertex table name 'users', got %s", vertexTables[0].Name)
	}

	// Test listing edge tables
	edgeTables := manager.ListTablesByKind(EdgeTable)
	if len(edgeTables) != 1 {
		t.Errorf("Expected 1 edge table, got %d", len(edgeTables))
	}

	if edgeTables[0].Name != "friendships" {
		t.Errorf("Expected edge table name 'friendships', got %s", edgeTables[0].Name)
	}
}

func TestManager_CreateIndex(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString},
		{Name: "email", Type: TypeString},
	}

	// Create table first
	tableReq := CreateTableRequest{
		Name:   "users",
		Kind:   VertexTable,
		Schema: schema,
	}
	_, err = manager.CreateTable(tableReq)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test creating index
	indexReq := CreateIndexRequest{
		Name:      "idx_users_name",
		TableName: "users",
		Type:      IndexBTree,
		Columns:   []string{"name"},
		IsUnique:  false,
	}

	index, err := manager.CreateIndex(indexReq)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if index.Name != "idx_users_name" {
		t.Errorf("Expected index name 'idx_users_name', got %s", index.Name)
	}

	if index.TableName != "users" {
		t.Errorf("Expected table name 'users', got %s", index.TableName)
	}

	// Check if index file was created
	if _, err := os.Stat(index.FilePath); os.IsNotExist(err) {
		t.Fatal("Index file was not created")
	}

	// Test creating duplicate index
	_, err = manager.CreateIndex(indexReq)
	if err != ErrIndexExists {
		t.Errorf("Expected ErrIndexExists, got %v", err)
	}

	// Test creating index on non-existent table
	invalidIndexReq := CreateIndexRequest{
		Name:      "idx_invalid",
		TableName: "non_existent",
		Type:      IndexBTree,
		Columns:   []string{"name"},
	}
	_, err = manager.CreateIndex(invalidIndexReq)
	if err != ErrTableNotFound {
		t.Errorf("Expected ErrTableNotFound, got %v", err)
	}

	// Test creating index with invalid columns
	invalidColumnsReq := CreateIndexRequest{
		Name:      "idx_invalid_cols",
		TableName: "users",
		Type:      IndexBTree,
		Columns:   []string{"non_existent_column"},
	}
	_, err = manager.CreateIndex(invalidColumnsReq)
	if err == nil {
		t.Fatal("Expected error when creating index with invalid columns")
	}
}

func TestManager_DropIndex(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	schema := []Column{
		{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
		{Name: "name", Type: TypeString},
	}

	// Create table and index
	tableReq := CreateTableRequest{
		Name:   "users",
		Kind:   VertexTable,
		Schema: schema,
	}
	_, err = manager.CreateTable(tableReq)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	indexReq := CreateIndexRequest{
		Name:      "idx_users_name",
		TableName: "users",
		Type:      IndexBTree,
		Columns:   []string{"name"},
	}
	index, err := manager.CreateIndex(indexReq)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Test dropping index
	err = manager.DropIndex("idx_users_name")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Check if index file was removed
	if _, err := os.Stat(index.FilePath); !os.IsNotExist(err) {
		t.Fatal("Index file was not removed")
	}

	// Test dropping non-existent index
	err = manager.DropIndex("non_existent")
	if err != ErrIndexNotFound {
		t.Errorf("Expected ErrIndexNotFound, got %v", err)
	}
}

func TestManager_Validation(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	err := manager.Initialize()
	if err != nil {
		t.Fatalf("Failed to initialize catalog: %v", err)
	}

	// Test schema validation with duplicate column names
	invalidSchema := []Column{
		{Name: "id", Type: TypeInt},
		{Name: "id", Type: TypeString}, // Duplicate name
	}

	req := CreateTableRequest{
		Name:   "invalid_table",
		Kind:   VertexTable,
		Schema: invalidSchema,
	}

	_, err = manager.CreateTable(req)
	if err == nil {
		t.Fatal("Expected error when creating table with duplicate column names")
	}

	// Test schema validation with empty column name
	invalidSchema2 := []Column{
		{Name: "", Type: TypeInt}, // Empty name
	}

	req2 := CreateTableRequest{
		Name:   "invalid_table2",
		Kind:   VertexTable,
		Schema: invalidSchema2,
	}

	_, err = manager.CreateTable(req2)
	if err != ErrInvalidColumnName {
		t.Errorf("Expected ErrInvalidColumnName, got %v", err)
	}
}
