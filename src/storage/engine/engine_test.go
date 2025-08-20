package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog/mocks"
	"github.com/spf13/afero"
	"os"
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/stretchr/testify/assert"
)

func Test_getVertexTableFilePath(t *testing.T) {
	ans := getVertexTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/vertex/friends.tbl", ans)
}

func Test_getEdgeTableFilePath(t *testing.T) {
	ans := getEdgeTableFilePath("/var/lib/graphdb", "friends")

	assert.Equal(t, "/var/lib/graphdb/tables/edge/friends.tbl", ans)
}

func TestStorageEngine_CreateVertexTable(t *testing.T) {
	dir := t.TempDir()

	err := systemcatalog.InitSystemCatalog(dir, afero.NewOsFs())
	if err != nil {
		t.Fatalf("InitSystemCatalog failed: %v", err)
	}

	catalog, err := systemcatalog.New(dir, afero.NewOsFs(), &mocks.MockDataBufferPool{})
	if err != nil {
		t.Fatalf("New SystemCatalog failed: %v", err)
	}

	lockMgr := &mocks.MockLockManager{
		AllowLock: true,
	}

	se := &StorageEngine{
		catalog: catalog,
		fs:      afero.NewOsFs(),
		lock:    lockMgr,
	}

	tableName := "User"
	schema := storage.Schema{
		"id":   storage.Column{Type: "int"},
		"name": storage.Column{Type: "string"},
	}

	err = se.CreateVertexTable(1, tableName, schema)
	if err != nil {
		t.Fatalf("CreateVertexTable failed: %v", err)
	}

	tablePath := getVertexTableFilePath(dir, tableName)
	info, err := os.Stat(tablePath)
	if err != nil {
		t.Fatalf("expected table file to exist, got error: %v", err)
	}

	if info.IsDir() {
		t.Fatalf("expected table file to be a regular file, got dir")
	}

	// 6. Проверяем запись в SystemCatalog
	tblMeta, err := catalog.GetVertexTableMeta(tableName)
	if err != nil {
		t.Fatalf("expected table in catalog, got error: %v", err)
	}
	if tblMeta.Name != tableName {
		t.Errorf("expected table name %s, got %s", tableName, tblMeta.Name)
	}

	if catalog.CurrentVersion() == 0 {
		t.Errorf("expected SystemCatalog version > 0")
	}

	err = se.CreateVertexTable(2, tableName, schema)
	if err == nil {
		t.Fatalf("expected error when creating table with same name")
	}

	info2, err := os.Stat(tablePath)
	if err != nil || info2.Size() == 0 {
		t.Errorf("expected table file to persist after failed creation")
	}
}
