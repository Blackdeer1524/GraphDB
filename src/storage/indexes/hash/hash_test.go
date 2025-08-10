package hash

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_getRootMetadata_1(t *testing.T) {
	m := rootMetaData{
		magic:         [4]byte{'h', 'i', 'd', 'x'},
		globalDepth:   2,
		maxBucketSize: 2,
		directory:     []uint64{1, 2, 3, 4},
		checksum:      5,
	}

	data := rootPageMetadata(m)

	require.Equal(t, m, getRootPageMetadata(data))
}
