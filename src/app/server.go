package app

import (
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/Blackdeer1524/GraphDB/src/handlers"
	"github.com/Blackdeer1524/GraphDB/src/query"
	"github.com/Blackdeer1524/GraphDB/src/recovery"
	"github.com/gorilla/mux"
	"github.com/spf13/afero"
)

const (
	bufferpoolSize = 100
)

type Server struct {
	router   *mux.Router
	handler  *handlers.Handler
	executor *query.Executor
	logger   *recovery.TxnLogger
}

func NewServer() (*Server, error) {
	executor, _, _, logger, err := query.SetupExecutor(
		afero.NewOsFs(), "", bufferpoolSize, true,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to setup executor: %w", err)
	}

	var ticker atomic.Uint64

	handler := &handlers.Handler{
		Executor: executor,
		Logger:   logger,
		Ticker:   &ticker,
	}

	router := mux.NewRouter()
	handler.RegisterRoutes(router)

	server := &Server{
		router:   router,
		handler:  handler,
		executor: executor,
		logger:   logger,
	}

	return server, nil
}

func (s *Server) Start(addr string) error {
	log.Printf("Server starting on %s", addr)

	return http.ListenAndServe(addr, s.router)
}
