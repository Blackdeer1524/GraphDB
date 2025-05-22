package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Blackdeer1524/GraphDB/storage/graph"
)

func main() {
	const path = "./data/test_graph"

	var g *graph.Graph
	var err error

	if _, err = os.Stat(path); os.IsNotExist(err) {
		fmt.Println("Graph not found, creating new one...")
		g, err = graph.CreateGraph(path)
		if err != nil {
			log.Fatalf("CreateGraph error: %v", err)
		}
	} else {
		fmt.Println("Loading existing graph...")
		g, err = graph.LoadGraph(path)
		if err != nil {
			log.Fatalf("LoadGraph error: %v", err)
		}
	}

	err = g.CreateNodeTable("Person", []graph.Column{
		{Name: "name", Type: "string"},
		{Name: "age", Type: "int"},
	})
	if err != nil {
		log.Println("CreateNodeTable warning:", err)
	}

	err = g.CreateEdgeTable("Friend", []graph.Column{
		{Name: "since", Type: "string"},
	})
	if err != nil {
		log.Println("CreateEdgeTable warning:", err)
	}

	fmt.Println("== Tables ==")
	for _, t := range g.ListTables() {
		fmt.Printf("- %s (%s) -> %s; %s\n", t.Name, t.Kind, t.FilePath, t.Schema)
	}
}
