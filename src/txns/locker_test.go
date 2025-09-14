package txns

import (
	"fmt"
	"testing"
	"time"

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
		{GranularLockSharedIntentionExclusive, GranularLockIntentionExclusive},
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

func TestLockCompatibilityMatrix(t *testing.T) {
	lm := NewLockManager()

	// Test all combinations of granular lock modes
	lockModes := []GranularLockMode{
		GranularLockIntentionShared,
		GranularLockIntentionExclusive,
		GranularLockShared,
		GranularLockSharedIntentionExclusive,
		GranularLockExclusive,
	}

	testFunc := func(txnID1, txnID2 common.TxnID, mode1, mode2 GranularLockMode) {
		defer lm.Unlock(txnID1)
		defer lm.Unlock(txnID2)

		done1 := make(chan struct{})
		done2 := make(chan struct{})

		compatible := mode1.Compatible(mode2)

		firstStartedNotifier := make(chan struct{})
		go func() {
			ct := lm.LockCatalog(txnID1, mode1)
			require.NotNil(t, ct, "LockCatalog should succeed for txn 1")
			firstStartedNotifier <- struct{}{}
			done1 <- struct{}{}
		}()

		// * incompatible modes: we either die due to a deadlock prevention policy
		// or wait forever
		// * compatible modes: we both complete
		go func() {
			<-firstStartedNotifier
			ct := lm.LockCatalog(txnID2, mode2)
			// block on the lock or die trying
			if ct == nil {
				return
			}
			done2 <- struct{}{}
		}()

		if compatible {
			select {
			case <-done1:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("First goroutine did not complete in time")
			}

			select {
			case <-done2:
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Second goroutine did not complete in time")
			}
		} else {
			select {
			case <-done1:
				select {
				case <-done2:
					t.Fatal("Second goroutine should not have completed because of incompatible locks")
				case <-time.After(100 * time.Millisecond):
				}
			case <-done2:
				select {
				case <-done1:
					t.Fatal("Second goroutine should not have completed because of incompatible locks")
				case <-time.After(100 * time.Millisecond):
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("None of the goroutine did not complete in time")
			}
		}
	}

	testIndex := 0
	for _, mode1 := range lockModes {
		for _, mode2 := range lockModes {
			t.Run(fmt.Sprintf("%s_vs_%s", mode1.String(), mode2.String()), func(t *testing.T) {
				testFunc(common.TxnID(testIndex*2+1), common.TxnID(testIndex*2+2), mode1, mode2)
				testFunc(common.TxnID(testIndex*2+2), common.TxnID(testIndex*2+1), mode1, mode2)
				testIndex++
			})
		}
	}
}
