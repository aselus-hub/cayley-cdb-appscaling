package main

import (
	"database/sql"
	"fmt"
	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/cayleygraph/cayley/graph/sql"
	"github.com/cayleygraph/cayley/quad"
	"github.com/satori/go.uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

const insecureCdbPath = "postgresql://root@127.0.0.1:26257/"

const runTime = 10*time.Second

type node struct {
	parent string
	child  string
	id     string
}

func generateAndSendAGraph(wg *sync.WaitGroup, sumChan chan uint64, quit chan struct{}) {
	defer wg.Done()
	var numProccessed uint64
	if store, err := InitCayley(insecureCdbPath + "cayley?sslmode=disable"); err != nil {
		fmt.Printf("Could not initialize cayley, %s\n", err.Error())
		return
	} else {
		defer store.Close()
		for {
			select {
			case <-quit:
				sumChan <- numProccessed
				return
			default:
				nodes := generateGraph(store)
				for _, n := range nodes {
					n = n
					addNquadsForNode(n, store)
					numProccessed++
				}
			}
		}
	}
}

func generateGraph(store *cayley.Handle) []node {
	// create a lineage for connecting A through link to B, etc.
	numItems := 2 + rand.Intn(10)
	// permutations := rand.Perm(numItems)
	// create a list of nodes, with number of true items and number of links.
	nodes := make([]node, numItems, numItems)
	currentUUID := ""
	for index := range nodes {
		newUuid := uuid.NewV4().String()
		nodes[index] = node{parent: currentUUID, id: uuid.NewV4().String(), child: currentUUID}
		currentUUID = newUuid
	}
	return nodes
}

func addNquadsForNode(n node, store *cayley.Handle) {
	tx := cayley.NewTransaction()
	if n.parent != "" {
		tx.AddQuad(quad.Make(n.parent, "related_through", n.id, nil))
	}

	if n.child != "" {
		tx.AddQuad(quad.Make(n.id, "related_through", n.child, nil))
	}
	store.ApplyTransaction(tx)
}

const NumRoutines = 8

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	SetupCayleyInCdb()
	termination := sync.WaitGroup{}
	termination.Add(NumRoutines)
	var trueEnd time.Duration
	var totalSum uint64
	processedNodes := make(chan uint64, NumRoutines)
	quit := make(chan struct{})

	start := time.Now()
	for i := 0; i < NumRoutines; i++ {
		go generateAndSendAGraph(&termination, processedNodes, quit)
	}
	for time.Since(start) < runTime {
		time.Sleep(time.Millisecond * 10)
	}
	close(quit)
	termination.Wait()
	trueEnd = time.Since(start)

outerFor:
	for {
		select {
		case subSum := <-processedNodes:
			totalSum += subSum
		default:
			break outerFor
		}
	}

	fmt.Printf("time elapsed: %v\n", trueEnd)
	fmt.Printf("total processed: %v\n", totalSum)
	fmt.Printf("sets/second: %v\n", float64(totalSum) / trueEnd.Seconds())
}

func SetupCayleyInCdb() {
	db, err := sql.Open("postgres", insecureCdbPath+"system?sslmode=disable")
	if err != nil {
		log.Fatalf("Unable to initialize cockroach: %s", err.Error())
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS cayley")
	if err != nil {
		log.Fatalf("Unable to initialize cockroach: %s", err.Error())
	}

	if err = graph.InitQuadStore("sql", insecureCdbPath+"cayley?sslmode=disable", graph.Options{"flavor": "cockroach"}); err != nil {
		if err != graph.ErrDatabaseExists {
			panic(err.Error())
		}
	}

}

func InitCayley(pathToCockroachDB string) (store *cayley.Handle, err error) {
	store, err = cayley.NewGraph("sql", pathToCockroachDB, graph.Options{"flavor": "cockroach"})
	if err != nil {
		return
	}
	return
}
