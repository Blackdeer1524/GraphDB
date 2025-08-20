package systemcatalog

import (
	"github.com/Blackdeer1524/GraphDB/src/storage"
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
