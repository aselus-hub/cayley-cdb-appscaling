package main

import (
	"database/sql"
	"fmt"
	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	_ "github.com/aselus-hub/cayley/graph/sql"
	"github.com/cayleygraph/cayley/quad"
	"github.com/satori/go.uuid"
	"log"
	"math/rand"
	"sync"
	"time"
)

const insecureCdbPath = "postgresql://root@127.0.0.1:26257/"

const (
runTime = 1*time.Second
reportEvery = 5*time.Second
batchSize = 15
NumRoutines = 4
)

type node struct {
	parent string
	child  string
	id     string
}

func generateAndSendAGraph(wg *sync.WaitGroup, sumChan chan uint64, quit chan struct{}) {
	defer wg.Done()
	var numProccessed uint64
	lastReport := time.Now()
	if store, err := InitCayley(insecureCdbPath + "cayley?sslmode=disable&binary_parameters"); err != nil {
		fmt.Printf("Could not initialize cayley, %s\n", err.Error())
		return
	} else {
		defer store.Close()
		tx := cayley.NewTransaction()
		for {
			select {
			case <-quit:
				tx = commitTx(store, tx, true)
				sumChan <- numProccessed
				return
			default:
				if time.Since(lastReport) > reportEvery {
					sumChan <- numProccessed
					numProccessed = 0
					lastReport = time.Now()
				}
				nodes := generateGraph(store)
				for _, n := range nodes {
					n = n
					addNquadsForNode(n, tx)
					tx = commitTx(store, tx, false)
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

func addNquadsForNode(n node, tx *graph.Transaction)  {
	if n.parent != "" {
		tx.AddQuad(quad.Make(n.parent, "related_through", n.id, nil))
	}

	if n.child != "" {
		tx.AddQuad(quad.Make(n.id, "related_through", n.child, nil))
	}
}

func commitTx(store *cayley.Handle, tx *graph.Transaction, force bool) *graph.Transaction {
	if len(tx.Deltas) > batchSize || force {
		err := store.ApplyTransaction(tx)
		if err != nil {
			panic(err.Error())
		}
		return cayley.NewTransaction()
	} else {
		return tx
	}
}

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


	var curSum uint64
	var numSum int

	lastUpdate := time.Now()
	for {
		select {
		case <-time.After(time.Second):
		case subSum := <-processedNodes:
			curSum += subSum
			fmt.Printf("node sum: %d\n", subSum)
			numSum++
			if numSum == NumRoutines {
				fmt.Printf("\trunning sum: %d [%v/sec]\n", curSum, float64(curSum) / time.Since(lastUpdate).Seconds())
				lastUpdate = time.Now()
				totalSum += curSum
				curSum = 0
				numSum = 0
			}
		}
		if time.Since(start) > runTime {
			close(quit)
			break
		}

	}

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
	fmt.Printf("total processed: %v\n", totalSum+curSum)
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
