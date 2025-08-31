package app

import (
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

const (
	EnvDev  = "dev"
	EnvProd = "prod"
)

type envVars struct {
	Environment string `split_words:"true"`

	ServerHost string `required:"true" split_words:"true"`
	ServerPort int    `required:"true" split_words:"true"`

	NodesAddr []string `required:"true" split_words:"true"`
}

func mustLoadEnv() envVars {
	var env envVars

	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	envconfig.MustProcess("GRAPHDB", &env)

	if env.Environment != "" && env.Environment != EnvDev && env.Environment != EnvProd {
		panic("invalid environment")
	} else if env.Environment == "" {
		env.Environment = EnvDev
	}

	if len(env.NodesAddr) < 1 {
		panic("invalid nodes addr len, must be at least 1")
	}

	return env
}
