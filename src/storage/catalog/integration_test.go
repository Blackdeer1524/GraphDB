package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

// TestIntegration demonstrates a complete workflow with the system catalog
func TestIntegration(t *testing.T) {
	tempDir := t.TempDir()
	manager := NewManager(tempDir)

	t.Run("InitializeCatalog", func(t *testing.T) {
		err := manager.Initialize()
		if err != nil {
			t.Fatalf("Failed to initialize catalog: %v", err)
		}

		// Verify catalog file was created
		catalogPath := filepath.Join(tempDir, CatalogFile)
		if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
			t.Fatal("Catalog file was not created")
		}
	})

	t.Run("Create vertex table", func(t *testing.T) {
		userSchema := []Column{
			{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
			{Name: "name", Type: TypeString, Nullable: false},
			{Name: "email", Type: TypeString, Nullable: false, IsUnique: true},
			{Name: "age", Type: TypeInt, Nullable: true},
			{Name: "created_at", Type: TypeDate, Nullable: false},
		}

		userTable, err := manager.CreateTable(CreateTableRequest{
			Name:        "users",
			Kind:        VertexTable,
			Schema:      userSchema,
			Description: "User vertices in the social graph",
		})
		if err != nil {
			t.Fatalf("Failed to create users table: %v", err)
		}

		if userTable.Name != "users" {
			t.Errorf("Expected table name 'users', got %s", userTable.Name)
		}

		if len(userTable.Schema) != 5 {
			t.Errorf("Expected 5 columns, got %d", len(userTable.Schema))
		}

		// Verify table file was created
		if _, err := os.Stat(userTable.FilePath); os.IsNotExist(err) {
			t.Fatal("Table file was not created")
		}
	})

	t.Run("Create edge table", func(t *testing.T) {
		friendshipSchema := []Column{
			{Name: "id", Type: TypeInt, IsPrimary: true, Nullable: false},
			{Name: "from_user_id", Type: TypeInt, Nullable: false},
			{Name: "to_user_id", Type: TypeInt, Nullable: false},
			{Name: "created_at", Type: TypeDate, Nullable: false},
			{Name: "status", Type: TypeString, Nullable: false},
		}

		friendshipTable, err := manager.CreateTable(CreateTableRequest{
			Name:        "friendships",
			Kind:        EdgeTable,
			Schema:      friendshipSchema,
			Description: "Friendship edges between users",
		})
		if err != nil {
			t.Fatalf("Failed to create friendships table: %v", err)
		}

		if friendshipTable.Name != "friendships" {
			t.Errorf("Expected table name 'friendships', got %s", friendshipTable.Name)
		}

		// Verify table file was created
		if _, err := os.Stat(friendshipTable.FilePath); os.IsNotExist(err) {
			t.Fatal("Table file was not created")
		}
	})

	t.Run("Create indexes", func(t *testing.T) {
		indexes := []CreateIndexRequest{
			{
				Name:      "idx_users_email",
				TableName: "users",
				Type:      IndexBTree,
				Columns:   []string{"email"},
				IsUnique:  true,
			},
			{
				Name:      "idx_users_name",
				TableName: "users",
				Type:      IndexBTree,
				Columns:   []string{"name"},
				IsUnique:  false,
			},
			{
				Name:      "idx_friendships_from",
				TableName: "friendships",
				Type:      IndexBTree,
				Columns:   []string{"from_user_id"},
				IsUnique:  false,
			},
			{
				Name:      "idx_friendships_to",
				TableName: "friendships",
				Type:      IndexBTree,
				Columns:   []string{"to_user_id"},
				IsUnique:  false,
			},
		}

		for _, indexReq := range indexes {
			index, err := manager.CreateIndex(indexReq)
			if err != nil {
				t.Fatalf("Failed to create index %s: %v", indexReq.Name, err)
			}

			// Verify index file was created
			if _, err := os.Stat(index.FilePath); os.IsNotExist(err) {
				t.Fatalf("Index file was not created for %s", index.Name)
			}
		}
	})

	t.Run("Verify catalog state", func(t *testing.T) {
		// Verify all tables are listed
		tables := manager.ListTables()
		if len(tables) != 2 {
			t.Errorf("Expected 2 tables in list, got %d", len(tables))
		}

		// Verify all indexes are listed
		indexes := manager.ListIndexes()
		if len(indexes) != 4 {
			t.Errorf("Expected 4 indexes in list, got %d", len(indexes))
		}

		// Verify indexes for specific table
		userIndexes := manager.ListIndexesByTable("users")
		if len(userIndexes) != 2 {
			t.Errorf("Expected 2 indexes for users table, got %d", len(userIndexes))
		}

		friendshipIndexes := manager.ListIndexesByTable("friendships")
		if len(friendshipIndexes) != 2 {
			t.Errorf("Expected 2 indexes for friendships table, got %d", len(friendshipIndexes))
		}
	})

	t.Run("Test persistence and reload", func(t *testing.T) {
		// Create new manager and load existing catalog
		newManager := NewManager(tempDir)
		err := newManager.Load()
		if err != nil {
			t.Fatalf("Failed to load catalog: %v", err)
		}

		// Verify table data was preserved
		usersTable, err := newManager.GetTable("users", VertexTable)
		if err != nil {
			t.Fatalf("Failed to get users table after reload: %v", err)
		}

		if len(usersTable.Schema) != 5 {
			t.Errorf("Expected 5 columns after reload, got %d", len(usersTable.Schema))
		}

		// Verify index data was preserved
		emailIndex, err := newManager.GetIndex("idx_users_email")
		if err != nil {
			t.Fatalf("Failed to get email index after reload: %v", err)
		}

		if emailIndex.TableName != "users" {
			t.Errorf("Expected table name 'users' for email index, got %s", emailIndex.TableName)
		}

		if !emailIndex.IsUnique {
			t.Error("Expected email index to be unique")
		}
	})

	t.Run("Test cleanup operations", func(t *testing.T) {
		// Drop an index
		err := manager.DropIndex("idx_users_name")
		if err != nil {
			t.Fatalf("Failed to drop index: %v", err)
		}

		// Verify index was removed
		indexes := manager.ListIndexes()
		if len(indexes) != 3 {
			t.Errorf("Expected 3 indexes after dropping one, got %d", len(indexes))
		}

		// Drop a table
		err = manager.DropTable("friendships", EdgeTable)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		// Verify table was removed
		tables := manager.ListTables()
		if len(tables) != 1 {
			t.Errorf("Expected 1 table after dropping one, got %d", len(tables))
		}

		// Verify associated indexes were also removed
		indexes = manager.ListIndexes()
		if len(indexes) != 1 {
			t.Errorf("Expected 1 index after dropping table, got %d", len(indexes))
		}

		// Verify only users table indexes remain
		userIndexes := manager.ListIndexesByTable("users")
		if len(userIndexes) != 1 {
			t.Errorf("Expected 1 index for users table, got %d", len(userIndexes))
		}
	})
}
