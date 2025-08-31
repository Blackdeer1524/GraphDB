package engine

import (
	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/storage"
)

type DirectoryItemInternalFields struct {
	ID         storage.DirItemID
	NextItemID storage.DirItemID
	PrevItemID storage.DirItemID
}

func NewDirectoryItemInternalFields(
	ID storage.DirItemID,
	NextItemID storage.DirItemID,
	PrevItemID storage.DirItemID,
) DirectoryItemInternalFields {
	return DirectoryItemInternalFields{
		ID:         ID,
		NextItemID: NextItemID,
		PrevItemID: PrevItemID,
	}
}

type DirectoryItemGraphFields struct {
	VertexID   storage.VertexID
	EdgeFileID common.FileID
	EdgeID     storage.EdgeID
}

func NewDirectoryItemGraphFields(
	VertexID storage.VertexID,
	EdgeFileID common.FileID,
	EdgeID storage.EdgeID,
) DirectoryItemGraphFields {
	return DirectoryItemGraphFields{
		VertexID:   VertexID,
		EdgeFileID: EdgeFileID,
		EdgeID:     EdgeID,
	}
}

type DirectoryItem struct {
	DirectoryItemInternalFields
	DirectoryItemGraphFields
}

type EdgeInternalFields struct {
	ID              storage.EdgeID
	DirectoryItemID storage.DirItemID
	SrcVertexID     storage.VertexID
	DstVertexID     storage.VertexID
	NextEdgeID      storage.EdgeID
	PrevEdgeID      storage.EdgeID
}

func NewEdgeInternalFields(
	ID storage.EdgeID,
	DirectoryItemID storage.DirItemID,
	SrcVertexID storage.VertexID,
	DstVertexID storage.VertexID,
	PrevEdgeID storage.EdgeID,
	NextEdgeID storage.EdgeID,
) EdgeInternalFields {
	return EdgeInternalFields{
		ID:              ID,
		DirectoryItemID: DirectoryItemID,
		SrcVertexID:     SrcVertexID,
		DstVertexID:     DstVertexID,
		NextEdgeID:      NextEdgeID,
		PrevEdgeID:      PrevEdgeID,
	}
}

type VertexInternalFields struct {
	ID        storage.VertexID
	DirItemID storage.DirItemID
}

func NewVertexInternalFields(
	ID storage.VertexID,
	DirItemID storage.DirItemID,
) VertexInternalFields {
	return VertexInternalFields{
		ID:        ID,
		DirItemID: DirItemID,
	}
}
