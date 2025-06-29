package main

import (
	"context"

	"github.com/Blackdeer1524/GraphDB/cmd/server/app"
)

func main() {
	app.MustExecute(context.Background())
}
