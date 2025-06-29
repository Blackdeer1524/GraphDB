package catalog

import (
	"fmt"
	"log"
)

// Example demonstrates how to use the system catalog
func Example() {
	// Create a new catalog manager
	manager := NewManager("./data/graphdb")

	// Initialize the catalog
	if err := manager.Initialize(); err != nil {
		log.Fatalf("Failed to initialize catalog: %v", err)
	}

	// Create a vertex table for users
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
		log.Fatalf("Failed to create users table: %v", err)
	}

	fmt.Printf("Created table: %s (%s)\n", userTable.Name, userTable.Kind)

	// Create an edge table for friendships
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
		log.Fatalf("Failed to create friendships table: %v", err)
	}

	fmt.Printf("Created table: %s (%s)\n", friendshipTable.Name, friendshipTable.Kind)

	// Create indexes for better query performance
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
			log.Printf("Failed to create index %s: %v", indexReq.Name, err)
			continue
		}
		fmt.Printf("Created index: %s on table %s\n", index.Name, index.TableName)
	}

	// List all tables
	fmt.Printf("\nAll Tables:\n")
	tables := manager.ListTables()
	for _, table := range tables {
		fmt.Printf("- %s (%s)\n", table.Name, table.Kind)
		if table.Description != "" {
			fmt.Printf("  Description: %s\n", table.Description)
		}
	}

	// List all indexes
	fmt.Printf("\nAll Indexes:\n")
	indexesList := manager.ListIndexes()
	for _, index := range indexesList {
		fmt.Printf("- %s on %s (%s, unique: %t)\n",
			index.Name, index.TableName, index.Type, index.IsUnique)
	}

	// Get specific table information
	usersTable, err := manager.GetTable("users", VertexTable)
	if err != nil {
		log.Printf("Failed to get users table: %v", err)
	} else {
		fmt.Printf("\nUsers Table Schema:\n")
		for _, column := range usersTable.Schema {
			fmt.Printf("- %s: %s (primary: %t, nullable: %t, unique: %t)\n",
				column.Name, column.Type, column.IsPrimary, column.Nullable, column.IsUnique)
		}
	}

	// List indexes for a specific table
	fmt.Printf("\nIndexes for users table:\n")
	userIndexes := manager.ListIndexesByTable("users")
	for _, index := range userIndexes {
		fmt.Printf("- %s: %s\n", index.Name, index.Type)
	}
}
