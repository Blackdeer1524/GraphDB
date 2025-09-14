package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/pkg/common"
	"github.com/Blackdeer1524/GraphDB/src/query"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/Blackdeer1524/GraphDB/src/storage"
	"github.com/gorilla/mux"
)

type Handler struct {
	Executor *query.Executor
	Logger   *recovery.TxnLogger
	Ticker   *atomic.Uint64
}

func (h *Handler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/vertices", h.InsertVertex).Methods("POST")
	router.HandleFunc("/vertexType", h.CreateVertexType).Methods("POST")
	router.HandleFunc("/edgeType", h.CreateEdgeType).Methods("POST")

	// sow
	router.HandleFunc("/trianglesCount", h.GetTrianglesCount).Methods("GET")
	router.HandleFunc("/vertexesOnDepth", h.GetVertexesOnDepth).Methods("GET")
}

func (h *Handler) InsertVertex(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) CreateEdgeType(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) GetVertexesOnDepth(w http.ResponseWriter, r *http.Request) {

}

func (h *Handler) GetTrianglesCount(w http.ResponseWriter, r *http.Request) {
	vertTableName := r.URL.Query().Get("table")
	if vertTableName == "" {
		http.Error(w, "table name could not be empty", http.StatusBadRequest)

		return
	}

	var triangles uint64

	err := query.Execute(
		h.Ticker,
		h.Executor,
		h.Logger,
		func(txnID common.TxnID, e *query.Executor, logger common.ITxnLoggerWithContext) (err error) {
			triangles, err = e.GetAllTriangles(txnID, vertTableName, logger)
			if err != nil {
				return err
			}

			return nil
		},
		false,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	response := map[string]interface{}{
		"triangles_count": triangles,
		"table_name":      vertTableName,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)

		return
	}
}

func (h *Handler) CreateVertexType(w http.ResponseWriter, r *http.Request) {
	var request struct {
		TableName string         `json:"table_name"`
		Schema    storage.Schema `json:"schema"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)

		return
	}

	if request.TableName == "" {
		http.Error(w, "table_name is required", http.StatusBadRequest)

		return
	}

	var schema storage.Schema
	for _, col := range request.Schema {
		schema = append(schema, storage.Column{
			Name: col.Name,
			Type: col.Type,
		})
	}

	if len(schema) == 0 {
		http.Error(w, "schema is required", http.StatusBadRequest)

		return
	}

	err := query.Execute(
		h.Ticker,
		h.Executor,
		h.Logger,
		func(txnID common.TxnID, e *query.Executor, logger common.ITxnLoggerWithContext) error {
			err := e.CreateVertexType(txnID, request.TableName, schema, logger)
			if err != nil {
				return fmt.Errorf("failed to create vertex type: %w", err)
			}

			return nil
		},
		false,
	)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	response := map[string]interface{}{
		"success": true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
