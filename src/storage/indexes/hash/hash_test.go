package hash

import (
	"testing"

	"github.com/Blackdeer1524/GraphDB/src/storage/page"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getRootMetadata(t *testing.T) {
	t.Run("SimpleTest", func(t *testing.T) {
		m := rootMetaData{
			magic:         [4]byte{'h', 'i', 'd', 'x'},
			globalDepth:   2,
			maxBucketSize: 2,
			directory:     []uint64{1, 2, 3, 4},
			checksum:      5,
		}

		data := rootPageMetadata(m)

		require.Equal(t, m, getRootPageMetadata(data))
	})
}

func TestBucketPageMarshalUnmarshal(t *testing.T) {
	t.Run("NormalBucket", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 3,
			entries: []KeyWithRID[uint64]{
				{key: 42, rid: RID{PageID: 1, SlotID: 2}},
				{key: 100, rid: RID{PageID: 3, SlotID: 4}},
				{key: 999, rid: RID{PageID: 5, SlotID: 6}},
			},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Equal(t, len(bucket.entries), len(result.entries), "Number of entries should match")
		for i, entry := range bucket.entries {
			assert.Equal(t, entry.key, result.entries[i].key, "Key at index %d should match", i)
			assert.Equal(t, entry.rid, result.entries[i].rid, "RID at index %d should match", i)
		}
	})

	t.Run("EmptyBucket", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 1,
			entriesCnt: 0,
			entries:    []KeyWithRID[uint64]{},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Empty(t, result.entries, "Entries should be empty")
	})

	t.Run("ExceedsPageSize", func(t *testing.T) {
		bucket := BucketPage[uint64]{
			localDepth: 2,
			entriesCnt: 1000,
			entries:    make([]KeyWithRID[uint64], 1000),
		}
		for i := 0; i < 1000; i++ {
			bucket.entries[i] = KeyWithRID[uint64]{
				key: uint64(i),
				rid: RID{PageID: uint64(i + 1), SlotID: uint16(i + 1)},
			}
		}

		_, err := bucketPageToBytes(bucket)
		assert.Error(t, err, "Serialization should fail due to size")
		assert.Contains(t, err.Error(), "bucket data exceeds page size")
	})

	t.Run("SingleEntry", func(t *testing.T) {
		// Подготавливаем BucketPage с одной записью
		bucket := BucketPage[uint64]{
			localDepth: 3,
			entriesCnt: 1,
			entries: []KeyWithRID[uint64]{
				{key: 12345, rid: RID{PageID: 10, SlotID: 20}},
			},
		}

		data, err := bucketPageToBytes(bucket)
		assert.NoError(t, err, "Serialization should succeed")
		assert.Len(t, data, page.PageSize, "Serialized data should match page size")

		result := getBucketPage[uint64](data)

		assert.Equal(t, bucket.localDepth, result.localDepth, "localDepth should match")
		assert.Equal(t, bucket.entriesCnt, result.entriesCnt, "entriesCnt should match")
		assert.Equal(t, len(bucket.entries), len(result.entries), "Number of entries should match")
		for i, entry := range bucket.entries {
			assert.Equal(t, entry.key, result.entries[i].key, "Key at index %d should match", i)
			assert.Equal(t, entry.rid, result.entries[i].rid, "RID at index %d should match", i)
		}
	})
}
