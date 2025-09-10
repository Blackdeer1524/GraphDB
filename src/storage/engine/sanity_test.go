package engine

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestPrepareFSforTable(t *testing.T) {
	fs := afero.NewMemMapFs()

	tableFilePath := "test.tbl"

	err := prepareFSforTable(fs, tableFilePath)
	require.NoError(t, err)

	file, err := fs.OpenFile(tableFilePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	require.NoError(t, err)

	testData := []byte("test data")
	_, err = file.WriteAt(testData, 0)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())

	// ================================
	err = prepareFSforTable(fs, tableFilePath)
	require.NoError(t, err)

	file, err = fs.OpenFile(tableFilePath, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)

	newData := make([]byte, len(testData))
	_, err = file.ReadAt(newData, 0)
	require.ErrorIs(t, err, io.EOF)
}
