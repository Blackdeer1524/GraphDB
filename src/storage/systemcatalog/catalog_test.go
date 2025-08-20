package systemcatalog

import (
	"encoding/json"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/Blackdeer1524/GraphDB/src/storage/systemcatalog/mocks"
	"github.com/spf13/afero"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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

	p.Insert(utils.ToBytes(uint64(0)))

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
		bp:                 &mocks.MockDataBufferPool{},

		mu: new(sync.RWMutex),
	}

	err := m.Save()
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

	p.Insert(utils.ToBytes(uint64(0)))

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
		bp:                 &mocks.MockDataBufferPool{},

		mu: new(sync.RWMutex),
	}

	err := m.Save()
	require.NoError(t, err)

	err = m.Save()
	require.NoError(t, err)

	require.Equal(t, uint64(2), m.currentVersion)

	for i := 1; i <= 2; i++ {
		fname := getSystemCatalogFilename(dir, uint64(i))

		_, err = os.Stat(fname)
		require.NoError(t, err)
	}
}
