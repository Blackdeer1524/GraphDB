package bufferpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRUReplacerBasic(t *testing.T) {
	r := NewLRUReplacer()

	r.Unpin(1)
	r.Unpin(2)
	r.Unpin(3)

	assert.Equal(t, uint64(3), r.GetSize())

	r.Pin(2)
	assert.Equal(t, uint64(2), r.GetSize())

	victim, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), victim)

	assert.Equal(t, uint64(1), r.GetSize())

	r.Unpin(4)
	r.Unpin(5)

	assert.Equal(t, uint64(3), r.GetSize())

	v1, _ := r.ChooseVictim()
	v2, _ := r.ChooseVictim()

	assert.ElementsMatch(t, []uint64{3, 4}, []uint64{v1, v2})

	assert.Equal(t, uint64(1), r.GetSize())
}

func TestLRUChooseVictimEmpty(t *testing.T) {
	r := NewLRUReplacer()

	_, err := r.ChooseVictim()
	assert.Error(t, err)
}
