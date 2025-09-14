package disk

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage/page"
)

func BenchmarkDiskManager(b *testing.B) {
	fs := afero.NewOsFs()
	basePath := b.TempDir()
	newPageFunc := func(fileID common.FileID, pageID common.PageID) page.SlottedPage {
		return page.NewSlottedPage()
	}
	diskManager := New(basePath, newPageFunc, fs)
	page := page.NewSlottedPage()

	pageIdents := make([]common.PageIdentity, b.N)
	for i := 0; i < b.N; i++ {
		pageIdents[i] = common.PageIdentity{
			FileID: 1,
			PageID: common.PageID(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page.Lock()
		require.NoError(b, diskManager.WritePageAssumeLocked(&page, pageIdents[i]))
		page.Unlock()
	}
}
