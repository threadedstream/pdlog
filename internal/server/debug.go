package server

import (
	"context"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type DebugServer struct {
	debugPort int
	server    *http.Server
	logger    *zap.Logger
}

func NewDebugServer(debugPort int, logger *zap.Logger) *DebugServer {
	return &DebugServer{debugPort: debugPort, logger: logger}
}

func (s *DebugServer) Run() {
	go func() {
		if err := s.runDebugServer(); err != nil {
			s.logger.Error("failed to run debug server", zap.Error(err))
		} else {
			s.logger.Info("debug server is started successfully")
		}
	}()
}

func (s *DebugServer) runDebugServer() error {
	router := chi.NewRouter()

	router.Mount("/debug", middleware.Profiler())

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.debugPort),
		Handler: router,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("failed to run debug server", zap.Error(err))
		}
	}()
	return nil
}

func (s *DebugServer) Shutdown() error {
	ctxWithTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.server.Shutdown(ctxWithTimeout)
}
