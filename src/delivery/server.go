package delivery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Blackdeer1524/GraphDB/src"
)

type Server struct {
	Host string
	Port int

	log  src.Logger
	http *http.Server
}

func NewServer(host string, port int, log src.Logger) *Server {
	return &Server{
		Host: host,
		Port: port,
		log:  log,
	}
}

func (s *Server) Run() error {
	mux := http.DefaultServeMux

	s.http = &http.Server{
		Addr: fmt.Sprintf(
			"%s:%d",
			s.Host,
			s.Port,
		),
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 10,
	}

	s.log.Infof(
		"Server is running on %s:%d",
		s.Host,
		s.Port,
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
