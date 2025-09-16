package delivery

import (
	"context"
	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/generated/api"
	"github.com/Blackdeer1524/GraphDB/src/generated/proto"
	"go.uber.org/zap"
)

type APIHandler struct {
	Client proto.RaftServiceClient
	Logger src.Logger
}

func (h *APIHandler) RaftInsertVertex(ctx context.Context, req *api.InsertVertexRequest) (api.RaftInsertVertexRes, error) {
	body := &proto.InsertVertexRequest{
		TableName: req.Table,
		Vertex:    h.toVertex(req.Record),
	}

	resp, err := h.Client.InsertVertex(ctx, body)
	if err != nil {
		return &api.RaftInsertVertexServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}

	return &api.VertexIDResponse{
		ID: api.UUID([]byte(resp.GetVertexId())),
	}, nil
}

func (h *APIHandler) RaftInsertVertices(ctx context.Context, req *api.InsertVerticesRequest) (api.RaftInsertVerticesRes, error) {
	body := &proto.InsertVerticesRequest{
		TableName: req.Table,
		Vertices:  h.toVertexSlice(req.Records),
	}

	resp, err := h.Client.InsertVertices(ctx, body)
	if err != nil {
		return &api.RaftInsertVerticesServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}

	out := make([]api.UUID, 0, len(resp.GetVertexIds()))
	for _, id := range resp.GetVertexIds() {
		out = append(out, api.UUID([]byte(id)))
	}

	return &api.VertexIDsResponse{
		Ids: out,
	}, nil
}

func (h *APIHandler) RaftInsertEdge(ctx context.Context, req *api.InsertEdgeRequest) (api.RaftInsertEdgeRes, error) {
	body := &proto.InsertEdgeRequest{
		TableName: req.Table,
		Edge:      h.toEdgeInfo(req.Edge),
	}

	resp, err := h.Client.InsertEdge(ctx, body)
	if err != nil {
		return &api.RaftInsertEdgeServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}

	return &api.EdgeIDResponse{
		ID: api.UUID([]byte(resp.GetEdgeId())),
	}, nil
}

func (h *APIHandler) RaftInsertEdges(ctx context.Context, req *api.InsertEdgesRequest) (api.RaftInsertEdgesRes, error) {
	body := &proto.InsertEdgesRequest{
		TableName: req.Table,
		Edges:     h.toEdgeInfoSlice(req.Edges),
	}

	resp, err := h.Client.InsertEdges(ctx, body)
	if err != nil {
		return &api.RaftInsertEdgesServiceUnavailable{
			Code:    "NOT_LEADER",
			Message: err.Error(),
		}, nil
	}

	out := make([]api.UUID, 0, len(resp.GetEdgeIds()))
	for _, id := range resp.GetEdgeIds() {
		out = append(out, api.UUID([]byte(id)))
	}

	return &api.EdgeIDsResponse{
		Ids: out,
	}, nil
}

func (h *APIHandler) NewError(ctx context.Context, err error) *api.ErrorStatusCode {
	h.Logger.Errorw("internal server error", zap.Error(err))
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
