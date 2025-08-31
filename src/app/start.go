package app

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/Blackdeer1524/GraphDB/src"
	"github.com/Blackdeer1524/GraphDB/src/delivery"
	"github.com/Blackdeer1524/GraphDB/src/pkg/utils"
)

const CloseTimeout = 15 * time.Second

type APIEntrypoint struct {
	ConfigPath string
	Env        envVars

	s   *delivery.Server
	log src.Logger
}

func (e *APIEntrypoint) Init(_ context.Context) error {
	e.Env = mustLoadEnv()

	var log src.Logger
	if e.Env.Environment == EnvDev {
		log = utils.Must(zap.NewDevelopment()).Sugar()
	} else {
		log = utils.Must(zap.NewProduction()).Sugar()
	}

	e.log = log

	e.s = delivery.NewServer(e.Env.ServerHost, e.Env.ServerPort, log)

	return nil
}

func (e *APIEntrypoint) Run(_ context.Context) error {
	return e.s.Run()
}

func (e *APIEntrypoint) Close() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), CloseTimeout)
	defer cancel()

	if e.s != nil {
		err = e.s.Close(ctx)
	}

	if e.log != nil {
		if err != nil {
			e.log.Error("failed to close server", zap.Error(err))
		}

		logErr := e.log.Sync()
		if logErr != nil && err != nil {
			err = fmt.Errorf("%w, %w", err, logErr)
		} else if logErr != nil {
			err = logErr
		}
	}

	return
}
