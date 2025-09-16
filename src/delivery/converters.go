package delivery

import (
	"github.com/Blackdeer1524/GraphDB/src/generated/api"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func (h *APIHandler) toVertex(in api.VertexInfo) *proto.VertexInfo {
	var label *string
	if in.Label.Set {
		label = &in.Label.Value
	}

	var properties *proto.Properties
	if in.Properties.Set {
		a, err := utils.JXMapToAnyMap(in.Properties.Value)
		if err != nil {
			h.Logger.Errorw("failed to convert properties to any", zap.Error(err))
			return nil
		}
		properties = &proto.Properties{
			Data: a,
		}
	}

	return &proto.VertexInfo{
		Label:      label,
		Properties: properties,
	}
}

func (h *APIHandler) toVertexSlice(in []api.VertexInfo) []*proto.VertexInfo {
	out := make([]*proto.VertexInfo, len(in))
	for i := range in {
		out[i] = h.toVertex(in[i])
	}
	return out
}

func (h *APIHandler) toEdgeInfo(in api.EdgeInfo) *proto.EdgeInfo {
	var properties *proto.Properties
	if in.Properties.Set {
		a, err := utils.JXMapToAnyMap(in.Properties.Value)
		if err != nil {
			h.Logger.Errorw("failed to convert properties to any", zap.Error(err))
			return nil
		}
		properties = &proto.Properties{
			Data: a,
		}
	}

	return &proto.EdgeInfo{
		SourceVertexId: uuid.UUID(in.From).String(),
		TargetVertexId: uuid.UUID(in.To).String(),
		Properties:     properties,
	}
}

func (h *APIHandler) toEdgeInfoSlice(in []api.EdgeInfo) []*proto.EdgeInfo {
	out := make([]*proto.EdgeInfo, len(in))
	for i := range in {
		out[i] = h.toEdgeInfo(in[i])
	}
	return out
}
