package delivery

import (
	"context"
	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/raft"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"go.uber.org/zap"

	api "github.com/Blackdeer1524/GraphDB/src/generated"
)

type APIHandler struct {
	Node   Node
	Logger src.Logger
}

func (h *APIHandler) RaftInsertVertex(ctx context.Context, req *api.InsertVertexRequest) (api.RaftInsertVertexRes, error) {
	id, err := h.Node.InsertVertex(req.Table, toProps(req.Record))
	if err != nil {
		return &api.RaftInsertVertexServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}
	return &api.VertexIDResponse{
		ID: api.UUID(id),
	}, nil
}

func (h *APIHandler) RaftInsertVertices(ctx context.Context, req *api.InsertVerticesRequest) (api.RaftInsertVerticesRes, error) {
	ids, err := h.Node.InsertVertices(req.Table, toPropsSlice(req.Records))
	if err != nil {
		return &api.RaftInsertVerticesServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}
	out := make([]api.UUID, len(ids))
	for i, id := range ids {
		out[i] = api.UUID(id)
	}
	return &api.VertexIDsResponse{
		Ids: out,
	}, nil
}

func (h *APIHandler) RaftInsertEdge(ctx context.Context, req *api.InsertEdgeRequest) (api.RaftInsertEdgeRes, error) {
	edgeID, err := h.Node.InsertEdge(req.Table, toEdgeInfo(req.Edge))
	if err != nil {
		return &api.RaftInsertEdgeServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}
	return &api.EdgeIDResponse{
		ID: api.UUID(edgeID),
	}, nil
}

func (h *APIHandler) RaftInsertEdges(ctx context.Context, req *api.InsertEdgesRequest) (api.RaftInsertEdgesRes, error) {
	edgeIDs, err := h.Node.InsertEdges(req.Table, toEdgeInfoSlice(req.Edges))
	if err != nil {
		return &api.RaftInsertEdgesServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}
	out := make([]api.UUID, len(edgeIDs))
	for i, id := range edgeIDs {
		out[i] = api.UUID(id)
	}
	return &api.EdgeIDsResponse{
		Ids: out,
	}, nil
}

func (h *APIHandler) NewError(ctx context.Context, err error) *api.ErrorStatusCode {
	h.Logger.Error("internal server error", zap.Error(err))
	return &api.ErrorStatusCode{
		StatusCode: 500,
		Response: api.Error{
			Code:    "500",
			Message: "Internal Server Error",
			Details: api.OptErrorDetails{
				Set: false,
			},
		},
	}
}

func toProps(in api.VertexDocument) map[string]any {
	m := make(map[string]any)
	for k, v := range in.AdditionalProps {
		m[k] = v
	}
	if in.Label.Set {
		m["label"] = in.Label.Value
	}
	if props := in.Properties; props.Set {
		m["properties"] = props
	}
	return m
}

func toPropsSlice(in []api.VertexDocument) []map[string]any {
	out := make([]map[string]any, len(in))
	for i := range in {
		out[i] = toProps(in[i])
	}
	return out
}

func toEdgeInfo(e api.EdgeInfo) (out raft.InsertEdgeDTO) {
	m := make(map[string]any)
	if e.Properties.Set {
		for k, v := range e.Properties.Value {
			m[k] = v
		}
	}

	return raft.InsertEdgeDTO{
		SrcVertexID: storage.VertexSystemID(e.From),
		DstVertexID: storage.VertexSystemID(e.To),
		Data:        m,
	}
}

func toEdgeInfoSlice(in []api.EdgeInfo) (out []raft.InsertEdgeDTO) {
	out = make([]raft.InsertEdgeDTO, len(in))
	for i := range in {
		out[i] = toEdgeInfo(in[i])
	}
	return out
}
