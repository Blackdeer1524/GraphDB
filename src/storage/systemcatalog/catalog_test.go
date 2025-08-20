package systemcatalog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getSystemCatalogFilename(t *testing.T) {
	res := getSystemCatalogFilename("gg", 228)

	require.Equal(t, "gg/system_catalog_228.json", res)
}
