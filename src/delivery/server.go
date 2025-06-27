package delivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/cfg"
)

type Server struct {
	log  src.Logger
	http *http.Server
	cfg  cfg.ServerConfig
}

func NewServer(log src.Logger, cfg cfg.ServerConfig) *Server {
	return &Server{
		log: log,
		cfg: cfg,
	}
}

func (s *Server) Run() error {
	mux := http.DefaultServeMux

	s.http = &http.Server{
		Addr: fmt.Sprintf(
			"%s:%d",
			s.cfg.ServerHost,
			s.cfg.ServerPort,
		),
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 10,
	}

	s.log.Infof(
		"Server is running on %s:%d",
		s.cfg.ServerHost,
		s.cfg.ServerPort,
	)

	if err := s.http.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("Server.Run http.ListenAndServe: %w", err)
	}

	return nil
}

func (s *Server) Close(ctx context.Context) error {
	if s.http == nil {
		return nil
	}

	if err := s.http.Shutdown(ctx); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("Server.Close http.Shutdown: %w", err)
	}

	s.log.Info("Server is closed")

	return nil
}
