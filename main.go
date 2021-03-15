package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	collections "github.com/couchbase/PerfDocLoader/collections"
	"github.com/couchbase/PerfDocLoader/docgen"
	"github.com/couchbase/PerfDocLoader/indexgen"
	options "github.com/couchbase/PerfDocLoader/options"
)

func main() {
	options.ArgParse()
	options.PrintOptions()

	buckets := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		buckets = append(buckets, fmt.Sprintf("%bucket-%v", i))
	}

	if options.CollGen {
		for i := 1; i <= 10; i++ {
			collections.CreateCollections(buckets[i])
		}
	}

	time.Sleep(5 * time.Second)

	jsonDocs := make(map[string]interface{})

	log.Printf("....... Starting initial docloading phase........")
	if options.InitDocsPerColl > 0 {
		// Generage JSON's
		seed := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < options.InitDocsPerColl; i++ {
			docId := fmt.Sprintf("Users-%s-%s", docgen.String(15, seed), i)
			jsonDocs[docId] = docgen.GenerateJson()
		}

		for i := 1; i <= 10; i++ {
			var wg sync.WaitGroup
			for i := 1; i <= options.NumColl; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					collections.PushDocs(buckets[i], jsonDocs, index, false)
				}(i)
			}
			wg.Wait()
		}
	}

	log.Printf("........ Done with initial docloading phase ........")

	if options.IndexGen {
		var bucketWg sync.WaitGroup
		for i := 1; i <= 10; i++ {
			bucketWg.Add(1)
			go func(index int) {
				defer bucketWg.Done()
				bucketn := fmt.Sprintf("bucket-%v", index)
				for j := 1; j <= options.NumColl; j++ {
					coll := fmt.Sprintf("%s-%v", options.CollPrefix, j)
					indexgen.CreateIndexes(bucketn, options.Scope, coll)
				}

				defnIDs := make([]uint64, 0)
				for i := 1; i <= options.NumColl; i++ {
					indexes := make([]string, 0)
					for j := 0; j < 10; j++ {
						indexes = append(indexes, fmt.Sprintf("%s:%s:%s-%v:index-%v", options.Bucket, options.Scope, options.CollPrefix, i, j))
					}
					defnIds := indexgen.BuildIndexes(indexes)
					defnIDs = append(defnIDs, defnIds...)
				}
				log.Printf("............ Waiting for all indexes to become active ............")
				indexgen.WaitTillAllIndxesActive(defnIDs)
				log.Printf("............ All indexes are active ............")
			}(i)
		}
	}

	time.Sleep(5 * time.Second)
	log.Printf("........ Starting incremental docloading phase ........")
	if options.InitDocsPerColl > 0 && options.IncrOpsPerSec > 0 {
		opsPerColl := options.IncrOpsPerSec / options.NumColl
		newDocs := make(map[string]interface{})
		i := 0
		for key, value := range jsonDocs {
			newDocs[key] = value
			i++
			if i >= opsPerColl {
				break
			}
		}
		log.Printf("........ OpsPerColl: %v, len(newDocs): %v ........", opsPerColl, len(newDocs))

		var wg sync.WaitGroup
		for i := 0; i < options.NumColl; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				newDocsCopy := make(map[string]interface{})
				for docId, doc := range newDocs {
					doc1 := make(map[string]interface{})
					docOrig := doc.(map[string]interface{})
					for key, value := range docOrig {
						doc1[key] = value
					}
					newDocsCopy[docId] = doc1
				}
				collections.PushDocs("bucket-0", newDocsCopy, index, true)
			}(i)
		}

		wg.Wait()
	}
}
