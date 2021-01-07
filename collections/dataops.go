package collections

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/PerfDocLoader/options"
	"github.com/couchbase/gocb/v2"
)

func PushDocs(jsonDocs map[string]interface{}, index int, upsert bool) {
	collPrefix := options.CollPrefix
	collName := fmt.Sprintf("%s-%v", collPrefix, index)

	opts := gocb.ClusterOptions{
		Username: options.Username,
		Password: options.Password,
	}
	cluster, err := gocb.Connect(options.KVaddress, opts)
	if err != nil {
		panic(err)
	}
	err = cluster.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		panic(err)
	}

	// Connect to corresponding bucket
	bucket := cluster.Bucket("default")

	err = bucket.WaitUntilReady(10*time.Second, nil)
	if err != nil {
		panic(err)
	}

	// Get Collection manager
	collection := bucket.Collection(collName)

	age := 1000
	for {
		start := time.Now().UnixNano()

		if upsert {
			age++
			for key, value := range jsonDocs {
				doc := value.(map[string]interface{})
				doc["age"] = age
				jsonDocs[key] = doc
			}
		}

		for docId, doc := range jsonDocs {
			var err error
			if upsert {
				_, err = collection.Upsert(docId, doc, nil)

			} else {
				_, err = collection.Insert(docId, doc, nil)
			}
			if err != nil {
				panic(err)
			}
		}
		end := time.Now().UnixNano()
		if end-start < int64(time.Second) {
			log.Printf("............ Sleeping for %v nanoseconds")
			time.Sleep(time.Duration(end-start) * time.Nanosecond)
		}
		if !options.LoopIncr || !upsert {
			log.Printf("Exiting as loopIncr is set to false")
			return
		}
		fmt.Printf(".")
	}
}
