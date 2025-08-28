package systemcatalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/bufferpool"
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/disk"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func Test_getSystemCatalogFilename(t *testing.T) {
	res := getSystemCatalogFilename("gg", 228)

	require.Equal(t, "gg/system_catalog_228.json", res)
}

func Test_GetSystemCatalogVersionFileName(t *testing.T) {
	res := GetSystemCatalogVersionFileName("ggwp")

	require.Equal(t, "ggwp/CURRENT", res)
}

func Test_GetFileIDToPathMap(t *testing.T) {
	expected := uint64(11)

	sCat := &Manager{
		mu: new(sync.RWMutex),
		data: &Data{
			VertexTables: map[string]storage.VertexTable{
				"test1": {
					FileID: 1,
				},
				"test2": {
					FileID: 9,
				},
			},
			EdgeTables: map[string]storage.EdgeTable{
				"test3": {
					FileID: 2,
				},
				"test4": {
					FileID: 10,
				},
			},
			Indexes: map[string]storage.Index{
				"test5": {
					FileID: 3,
				},
				"test6": {
					FileID: 11,
				},
			},
		},
	}

	require.Equal(t, expected, calcMaxFileID(sCat.data))
}

func TestManager_Save_CreatesNewVersionFile(t *testing.T) {
	dir := t.TempDir()

	p := page.NewSlottedPage()
	p.UnsafeInsertNoLogs(utils.ToBytes(uint64(0)))

	pool := bufferpool.NewDebugBufferPool(
		bufferpool.New(10, bufferpool.NewLRUReplacer(), disk.NewInMemoryManager()),
		make(map[common.PageIdentity]struct{}),
	)

	m := &Manager{
		basePath: dir,
		fs:       afero.NewOsFs(),
		data: &Data{
			Metadata:     storage.Metadata{},
			VertexTables: map[string]storage.VertexTable{},
			EdgeTables:   map[string]storage.EdgeTable{},
			Indexes:      map[string]storage.Index{},
		},
		currentVersion:     0,
		currentVersionPage: p,
		bp:                 pool,

		mu: new(sync.RWMutex),
	}

	err := m.Save(common.NoLogs())
	require.NoError(t, err)
	require.Equal(t, uint64(1), m.currentVersion)

	fname := filepath.Join(dir, "system_catalog_1.json")
	data, err := os.ReadFile(fname)
	require.NoError(t, err)

	var restored Data
	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	require.Equal(t, m.data, &restored)
}

func TestManager_Save_Twice_IncrementsVersion(t *testing.T) {
	dir := t.TempDir()

	p := page.NewSlottedPage()

	p.UnsafeInsertNoLogs(utils.ToBytes(uint64(0)))

	m := &Manager{
		basePath: dir,
		fs:       afero.NewOsFs(),
		data: &Data{
			Metadata:     storage.Metadata{},
			VertexTables: map[string]storage.VertexTable{},
			EdgeTables:   map[string]storage.EdgeTable{},
			Indexes:      map[string]storage.Index{},
		},
		currentVersion:     0,
		currentVersionPage: p,
		bp: bufferpool.NewDebugBufferPool(
			bufferpool.New(10, bufferpool.NewLRUReplacer(), disk.NewInMemoryManager()),
			make(map[common.PageIdentity]struct{}),
		),

		mu: new(sync.RWMutex),
	}

	err := m.Save(common.NoLogs())
	require.NoError(t, err)

	err = m.Save(common.NoLogs())
	require.NoError(t, err)

	require.Equal(t, uint64(2), m.currentVersion)

	for i := 1; i <= 2; i++ {
		fname := getSystemCatalogFilename(dir, uint64(i))

		_, err = os.Stat(fname)
		require.NoError(t, err)
	}
}

func TestManager_updateSystemCatalogData(t *testing.T) {
	dir := t.TempDir()

	data := Data{
		Metadata: storage.Metadata{
			Version: "v228",
			Name:    "TestCatalog",
		},
		VertexTables: map[string]storage.VertexTable{
			"User": {
				Name: "User",
			},
		},
		EdgeTables: map[string]storage.EdgeTable{},
		Indexes:    map[string]storage.Index{},
	}

	filename := getSystemCatalogFilename(dir, 1)
	fileBytes, _ := json.Marshal(data)
	if err := os.WriteFile(filename, fileBytes, 0600); err != nil {
		t.Fatalf("failed to write catalog file: %v", err)
	}

	p := page.NewSlottedPage()
	p.UnsafeInsertNoLogs(utils.ToBytes(uint64(1)))

	m := &Manager{
		basePath:           dir,
		fs:                 afero.NewOsFs(),
		currentVersionPage: p,
		currentVersion:     0,

		mu: new(sync.RWMutex),
	}

	err := m.updateSystemCatalogData()
	require.NoError(t, err)

	require.Equal(t, uint64(1), m.currentVersion)

	_, ok := m.data.VertexTables["User"]
	require.True(t, ok)

	require.Equal(t, "v228", m.data.Metadata.Version)
}

func TestManager_updateSystemCatalogData_NoUpdate(t *testing.T) {
	dir := t.TempDir()

	data := Data{
		Metadata: storage.Metadata{
			Version: "v228",
			Name:    "TestCatalog",
		},
		VertexTables: map[string]storage.VertexTable{
			"User": {
				Name: "User",
			},
		},
		EdgeTables: map[string]storage.EdgeTable{},
		Indexes:    map[string]storage.Index{},
	}

	filename := getSystemCatalogFilename(dir, 1)
	fileBytes, _ := json.Marshal(data)
	if err := os.WriteFile(filename, fileBytes, 0600); err != nil {
		t.Fatalf("failed to write catalog file: %v", err)
	}

	p := page.NewSlottedPage()
	p.UnsafeInsertNoLogs(utils.ToBytes(uint64(1)))

	m := &Manager{
		basePath:           dir,
		fs:                 afero.NewOsFs(),
		currentVersionPage: p,
		currentVersion:     1,
		data: &Data{
			Metadata: storage.Metadata{
				Version: "not-updated",
			},
		},

		mu: new(sync.RWMutex),
	}

	err := m.updateSystemCatalogData()
	require.NoError(t, err)

	require.Equal(t, uint64(1), m.currentVersion)

	require.Equal(t, "not-updated", m.data.Metadata.Version)
}
