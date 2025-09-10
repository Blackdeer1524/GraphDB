package txns

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestLockManagerNilCatalogLockToken(t *testing.T) {
	l := NewLockManager()
	txnID := common.TxnID(1)
	cToken := NewNilCatalogLockToken(txnID)

	require.True(t, l.UpgradeCatalogLock(cToken, GranularLockShared))
	require.Equal(t, GranularLockShared, cToken.lockMode)
	require.Equal(t, true, cToken.wasSetUp)
}

func TestLockManagerNilFileLockToken(t *testing.T) {
	l := NewLockManager()

	tests := []struct {
		fileLockMode            GranularLockMode
		expectedCatalogLockMode GranularLockMode
	}{
		{GranularLockExclusive, GranularLockIntentionExclusive},
		{GranularLockIntentionShared, GranularLockIntentionShared},
		{GranularLockIntentionExclusive, GranularLockIntentionExclusive},
		{GranularLockSharedIntentionExclusive, GranularLockSharedIntentionExclusive},
		{GranularLockShared, GranularLockIntentionShared},
	}

	for i, test := range tests {
		name := fmt.Sprintf(
			"fileLockMode: %s, expectedCatalogLockMode: %s",
			test.fileLockMode.String(),
			test.expectedCatalogLockMode.String(),
		)
		t.Run(name, func(t *testing.T) {
			defer l.Unlock(common.TxnID(i))
			cToken := NewNilCatalogLockToken(common.TxnID(i))
			fToken := NewNilFileLockToken(cToken, common.FileID(i))

			require.True(t, l.UpgradeFileLock(fToken, test.fileLockMode))
			require.Equal(t, test.fileLockMode, fToken.lockMode)
			require.Equal(t, true, fToken.wasSetUp)

			require.Equal(t, test.expectedCatalogLockMode, cToken.lockMode)
			require.Equal(t, true, cToken.wasSetUp)
		})
	}
}

func TestLockManagerNilPageLockToken(t *testing.T) {
	l := NewLockManager()

	tests := []struct {
		pageLockMode            PageLockMode
		expectedFileLockMode    GranularLockMode
		expectedCatalogLockMode GranularLockMode
	}{
		{PageLockExclusive, GranularLockIntentionExclusive, GranularLockIntentionExclusive},
		{PageLockShared, GranularLockIntentionShared, GranularLockIntentionShared},
	}

	for i, test := range tests {
		name := fmt.Sprintf(
			"pageLock: %s, expectedFileLock: %s, expectedCatalogLock: %s",
			test.pageLockMode.String(),
			test.expectedFileLockMode.String(),
			test.expectedCatalogLockMode.String(),
		)
		t.Run(name, func(t *testing.T) {
			cToken := NewNilCatalogLockToken(common.TxnID(i))
			fToken := NewNilFileLockToken(cToken, common.FileID(i))
			pToken := NewNilPageLockToken(
				fToken,
				common.PageIdentity{FileID: fToken.fileID, PageID: common.PageID(i)},
			)

			require.True(t, l.UpgradePageLock(pToken, test.pageLockMode))
			require.Equal(t, test.pageLockMode, pToken.lockMode)
			require.Equal(t, true, pToken.wasSetUp)

			require.Equal(t, test.expectedFileLockMode, fToken.lockMode)
			require.Equal(t, true, fToken.wasSetUp)

			require.Equal(t, test.expectedCatalogLockMode, cToken.lockMode)
			require.Equal(t, true, cToken.wasSetUp)
		})
	}
}

func TestLockManagerLockToken_RequestWeakerLockModeRequestGranted(t *testing.T) {
	l := NewLockManager()

	lockModes := []GranularLockMode{
		GranularLockShared,
		GranularLockIntentionShared,
		GranularLockIntentionExclusive,
		GranularLockSharedIntentionExclusive,
		GranularLockExclusive,
	}

	weakerLockModes := map[GranularLockMode][]GranularLockMode{}
	for _, strongerMode := range lockModes {
		weakerLockModes[strongerMode] = []GranularLockMode{}
		for _, weakerMode := range lockModes {
			if weakerMode.WeakerOrEqual(strongerMode) {
				weakerLockModes[strongerMode] = append(weakerLockModes[strongerMode], weakerMode)
			}
		}
	}

	txnID := common.TxnID(1)
	fileID := common.FileID(1)
	tests := []struct {
		name        string
		lockManager any
		lockFunc    func(txnID common.TxnID, lockMode GranularLockMode, ct *CatalogLockToken)
	}{
		{
			"catalog",
			l.catalogLockManager,
			func(txnID common.TxnID, lockMode GranularLockMode, _ *CatalogLockToken) {
				require.NotNil(t, l.LockCatalog(txnID, lockMode))
			},
		},
		{
			"file",
			l.fileLockManager,
			func(txnID common.TxnID, lockMode GranularLockMode, ct *CatalogLockToken) {
				require.NotNil(t, l.LockFile(ct, fileID, lockMode))
			},
		},
	}

	for _, test := range tests {
		for strongerMode, weakerModes := range weakerLockModes {
			for _, weakerMode := range weakerModes {
				name := fmt.Sprintf(
					"[%s] strongerMode: %s, weakerMode: %s",
					test.name,
					strongerMode.String(),
					weakerMode.String(),
				)
				t.Run(name, func(t *testing.T) {
					cToken := NewNilCatalogLockToken(txnID)
					// initial lock
					test.lockFunc(txnID, strongerMode, cToken)
					defer l.Unlock(txnID)
					// later lock request
					test.lockFunc(txnID, weakerMode, cToken)

					switch test.name {
					case "catalog":
						qAny, ok := test.lockManager.(*lockGranularityManager[GranularLockMode, struct{}]).qs.Load(
							struct{}{},
						)
						require.True(t, ok)

						q := qAny.(*txnQueue[GranularLockMode, struct{}])
						txnQueueEntryAny, ok := q.txnNodes.Load(txnID)
						require.True(t, ok)
						txnQueueEntry := txnQueueEntryAny.(*txnQueueEntry[GranularLockMode, struct{}])
						require.Equal(
							t,
							strongerMode.String(),
							txnQueueEntry.r.lockMode.String(),
						)
					case "file":
						qAny, ok := test.lockManager.(*lockGranularityManager[GranularLockMode, common.FileID]).qs.Load(
							fileID,
						)
						require.True(t, ok)

						q := qAny.(*txnQueue[GranularLockMode, common.FileID])
						txnQueueEntryAny, ok := q.txnNodes.Load(txnID)
						require.True(t, ok)
						txnQueueEntry := txnQueueEntryAny.(*txnQueueEntry[GranularLockMode, common.FileID])
						require.Equal(
							t,
							strongerMode.String(),
							txnQueueEntry.r.lockMode.String(),
						)
					}
				})
			}
		}
	}
}
