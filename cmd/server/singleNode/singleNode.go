package main

import (
	"log"

	"github.com/Blackdeer1524/GraphDB/src/app"
)

func main() {
	s, err := app.NewServer()
	if err != nil {
		log.Fatal(err)
	}

	s.Start("localhost:8080")
}
