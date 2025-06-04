package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Blackdeer1524/GraphDB/bufferpool"
	"github.com/Blackdeer1524/GraphDB/queryexecutor"
	"github.com/Blackdeer1524/GraphDB/storage/disk"
	"github.com/Blackdeer1524/GraphDB/storage/graph"
	"github.com/Blackdeer1524/GraphDB/storage/page"
)

const graphName = "test_graph"

func createFileIDToPath(g *graph.Graph) map[uint64]string {
	mapping := make(map[uint64]string)

	for i, table := range g.ListTables() {
		mapping[uint64(i)] = table.FilePath
	}

	return mapping
}

func main() {
	const path = "./data/" + graphName

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

	fileIDToPath := createFileIDToPath(g)

	diskMgr := disk.New(
		fileIDToPath,
		func(fileID, pageID uint64) *page.SlottedPage {
			return page.NewSlottedPage(fileID, pageID)
		},
	)

	bufferpool, _ := bufferpool.New(100, bufferpool.NewLRUReplacer(), diskMgr)

	qe := queryexecutor.QueryExecutor{
		Catalog:    g,
		BufferPool: bufferpool,
	}

	//qe.AppendRows("Person", graph.VertexTable, []queryexecutor.Row{
	//	{
	//		"name": "some",
	//		"age":  19,
	//	},
	//})

	req := queryexecutor.FindRequest{
		TableName: "Person",
		TableKind: graph.VertexTable,
		Filter: func(r queryexecutor.Row) bool {
			val, ok := r["age"]
			if !ok {
				return false
			}

			valInt, ok := val.(float64)
			if !ok {
				return false
			}

			return valInt == 18
		},
	}

	res, err := qe.FindVertices(req)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Print(res)
}
