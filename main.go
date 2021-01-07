package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	collections "github.com/couchbase/PerfDocLoader/collections"
	"github.com/couchbase/PerfDocLoader/docgen"
	options "github.com/couchbase/PerfDocLoader/options"
)

func main() {
	options.ArgParse()
	options.PrintOptions()

	if options.CollGen {
		collections.CreateCollections()
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

		var wg sync.WaitGroup
		for i := 0; i < options.NumColl; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				collections.PushDocs(jsonDocs, index, false)
			}(i)
		}
		wg.Wait()
	}

	time.Sleep(5 * time.Second)
	log.Printf("....... Starting incremental docloading phase........")
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

		var wg sync.WaitGroup
		for i := 0; i < options.NumColl; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				newDocsCopy := make(map[string]interface{})
				for docId, doc := range newDocs {
					newDocsCopy[docId] = doc
				}
				collections.PushDocs(newDocsCopy, index, true)
			}(i)
		}

		wg.Wait()
	}
}
