package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordIDSerializedSize(t *testing.T) {
	r := RecordID{
		FileID:  1,
		PageID:  2,
		SlotNum: 3,
	}
	b, err := r.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal record ID: %v", err)
	}
	assert.Equal(t, SerializedRecordIDSize, len(b))
}
