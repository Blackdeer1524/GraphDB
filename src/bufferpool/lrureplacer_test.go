package bufferpool

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestLRUReplacerBasic(t *testing.T) {
	r := NewLRUReplacer()

	fileID := common.FileID(42)
	first := common.PageIdentity{FileID: fileID, PageID: 1}
	second := common.PageIdentity{FileID: fileID, PageID: 2}
	third := common.PageIdentity{FileID: fileID, PageID: 3}
	fourth := common.PageIdentity{FileID: fileID, PageID: 4}
	fifth := common.PageIdentity{FileID: fileID, PageID: 5}

	r.Unpin(first)
	r.Unpin(second)
	r.Unpin(third)

	assert.Equal(t, uint64(3), r.GetSize())

	r.Pin(second)
	assert.Equal(t, uint64(2), r.GetSize())

	victim, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, first, victim)

	assert.Equal(t, uint64(1), r.GetSize())

	r.Unpin(fourth)
	r.Unpin(fifth)

	assert.Equal(t, uint64(3), r.GetSize())

	v1, _ := r.ChooseVictim()
	v2, _ := r.ChooseVictim()

	assert.ElementsMatch(
		t,
		[]common.PageIdentity{third, fourth},
		[]common.PageIdentity{v1, v2},
	)

	assert.Equal(t, uint64(1), r.GetSize())
}

func TestLRUChooseVictimEmpty(t *testing.T) {
	r := NewLRUReplacer()

	_, err := r.ChooseVictim()
	assert.Error(t, err)
}

func TestLRUReplacerConcurrentUnpin(t *testing.T) {
	r := NewLRUReplacer()

	const numFrames = 200
	fileID := common.FileID(7)

	var wg sync.WaitGroup
	wg.Add(numFrames)
	for i := 0; i < numFrames; i++ {
		i := i
		go func() {
			defer wg.Done()
			r.Unpin(common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}) //nolint:gosec
		}()
	}
	wg.Wait()

	assert.Equal(t, uint64(numFrames), r.GetSize())

	victims := make([]common.PageIdentity, 0, numFrames)
	for i := 0; i < numFrames; i++ {
		v, err := r.ChooseVictim()
		assert.NoError(t, err)
		victims = append(victims, v)
	}

	// Ensure all inserted frames were eventually returned as victims
	expected := make([]common.PageIdentity, 0, numFrames)
	for i := 0; i < numFrames; i++ {
		expected = append(
			expected,
			common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}, //nolint:gosec
		)
	}
	assert.ElementsMatch(t, expected, victims)
	assert.Equal(t, uint64(0), r.GetSize())
}

func TestLRUReplacerConcurrentPinAndUnpin(t *testing.T) {
	r := NewLRUReplacer()

	const initial = 150
	const added = 100
	fileID := common.FileID(13)

	// Seed with initial frames
	for i := 0; i < initial; i++ {
		r.Unpin(common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}) //nolint:gosec
	}
	assert.Equal(t, uint64(initial), r.GetSize())

	var wg sync.WaitGroup
	// Concurrently pin all initial frames (removing them)
	wg.Add(initial)
	for i := 0; i < initial; i++ {
		i := i
		go func() {
			defer wg.Done()
			r.Pin(common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}) //nolint:gosec
		}()
	}

	// Concurrently unpin a new disjoint set of frames
	wg.Add(added)
	for i := initial; i < initial+added; i++ {
		i := i
		go func() {
			defer wg.Done()
			r.Unpin(common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}) //nolint:gosec
		}()
	}

	wg.Wait()

	// All initial frames should be pinned (removed), and only the added ones remain
	assert.Equal(t, uint64(added), r.GetSize())

	// Collect victims and ensure they are exactly the added set
	victims := make([]common.PageIdentity, 0, added)
	for i := 0; i < added; i++ {
		v, err := r.ChooseVictim()
		assert.NoError(t, err)
		victims = append(victims, v)
	}
	expected := make([]common.PageIdentity, 0, added)
	for i := initial; i < initial+added; i++ {
		expected = append(
			expected,
			common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}, //nolint:gosec
		)
	}
	assert.ElementsMatch(t, expected, victims)
	assert.Equal(t, uint64(0), r.GetSize())
}

func TestLRUReplacerParallelChooseVictim(t *testing.T) {
	r := NewLRUReplacer()

	const numFrames = 128
	fileID := common.FileID(21)
	for i := 0; i < numFrames; i++ {
		r.Unpin(common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}) //nolint:gosec
	}

	var wg sync.WaitGroup
	victimsCh := make(chan common.PageIdentity, numFrames)
	errsCh := make(chan error, numFrames)

	wg.Add(numFrames)
	for i := 0; i < numFrames; i++ {
		go func() {
			defer wg.Done()
			v, err := r.ChooseVictim()
			if err != nil {
				errsCh <- err
				return
			}
			victimsCh <- v
		}()
	}

	wg.Wait()
	close(victimsCh)
	close(errsCh)

	// No errors expected because we seeded exactly numFrames
	for err := range errsCh {
		assert.NoError(t, err)
	}

	victims := make([]common.PageIdentity, 0, numFrames)
	for v := range victimsCh {
		victims = append(victims, v)
	}

	expected := make([]common.PageIdentity, 0, numFrames)
	for i := 0; i < numFrames; i++ {
		expected = append(
			expected,
			common.PageIdentity{FileID: fileID, PageID: common.PageID(i)}, //nolint:gosec
		)
	}
	assert.ElementsMatch(t, expected, victims)
	assert.Equal(t, uint64(0), r.GetSize())
}
