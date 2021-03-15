package collections

import (
	"fmt"
	"log"
	"time"

	"github.com/couchbase/PerfDocLoader/options"
	"github.com/couchbase/gocb/v2"
)

func CreateCollections(bucketn string) {
	// Connect to cluster
	log.Printf("Connecting to cluster: %v", options.KVaddress)
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
	log.Printf("Connected to cluster: %v", options.KVaddress)

	// Connect to corresponding bucket
	log.Printf("....Connecting to bucket: %v", bucketn)
	bucket := cluster.Bucket(bucketn)

	err = bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		panic(err)
	}
	log.Printf("....Connected to bucket: %v", options.Bucket)

	// Get Collection manager
	collManager := bucket.Collections()

	// Start creating collections
	for i := 1; i <= options.NumColl; i++ {
		collName := fmt.Sprintf("%s-%v", options.CollPrefix, i)
		collSpec := gocb.CollectionSpec{
			Name:      collName,
			ScopeName: options.Scope,
		}
		log.Printf("........ Creating collection: %v in scope: %v for bucket: %v", collName, options.Scope, bucketn)
		err := collManager.CreateCollection(collSpec, nil)
		if err != nil {
			log.Printf("[Error] %v", err)
		}
		log.Printf("........ Successfully created collection: %v in scope: %v for bucket: %v", collName, options.Scope, bucketn)
	}
}
