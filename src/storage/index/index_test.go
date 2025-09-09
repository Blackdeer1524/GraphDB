package index

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
)

func TestMarshalAndUnmarshalBucketItem(t *testing.T) {
	status := bucketItemStatusInserted
	key := "test"
	rid := common.RecordID{
		FileID:  1,
		PageID:  2,
		SlotNum: 3,
	}
	keySize := len(key)
	marshalled, err := marshalBucketItem(status, key, keySize, rid)
	assert.NoError(t, err)
	assert.Equal(t, int(bucketItemSizeWithoutKey)+keySize, len(marshalled))
}
