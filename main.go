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

	age := 1000
	if options.InitDocsPerColl > 0 && options.IncrOpsPerSec > 0 {
		opsPerColl := options.IncrOpsPerSec / options.NumColl
		newDocs := make(map[string]interface{})
		i := 0
		for key, value := range jsonDocs {
			newDocs[key] = value
			i++
			if i > opsPerColl {
				break
			}
		}

		for {
			start := time.Now().UnixNano()
			age++

			for key, value := range newDocs {
				doc := value.(map[string]interface{})
				doc["age"] = age
				newDocs[key] = doc
			}

			var wg sync.WaitGroup
			for i := 0; i < options.NumColl; i++ {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					collections.PushDocs(newDocs, index, true)
				}(i)
			}

			wg.Wait()
			end := time.Now().UnixNano()

			if end-start < int64(time.Second) {
				time.Sleep(time.Duration(end-start) * time.Nanosecond)
			}
			if !options.LoopIncr {
				log.Printf("Exiting as loopIncr is set to false")
				return
			}
			fmt.Printf(".")
		}
	}
}
