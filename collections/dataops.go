package collections

import (
	"fmt"
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
	err = cluster.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		panic(err)
	}

	// Connect to corresponding bucket
	bucket := cluster.Bucket("default")

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		panic(err)
	}

	// Get Collection manager
	collection := bucket.Collection(collName)

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
}
