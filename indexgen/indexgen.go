package indexgen

import (
	"fmt"
	"log"
	"strings"

	"github.com/couchbase/PerfDocLoader/options"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/querycmd"
	"github.com/couchbase/indexing/secondary/tests/framework/secondaryindex"
)

func createDeferIndex(index, bucket, scope, coll string, field []string) {
	err := secondaryindex.CreateSecondaryIndex3(index, bucket, scope, coll, options.IndexAddr,
		"", field, []bool{false}, false, []byte("{\"defer_build\": true}"), c.SINGLE, nil, true,
		0, nil)
	if err != nil {
		panic(err)
	}
}

func CreateIndexes(bucket, scope, coll string) {
	createDeferIndex("index-0", bucket, scope, coll, []string{"state"})
	createDeferIndex("index-1", bucket, scope, coll, []string{"age"})
	createDeferIndex("index-2", bucket, scope, coll, []string{"alt_email"})
	createDeferIndex("index-3", bucket, scope, coll, []string{"city"})
	createDeferIndex("index-4", bucket, scope, coll, []string{"coins"})
	createDeferIndex("index-5", bucket, scope, coll, []string{"country"})
	createDeferIndex("index-6", bucket, scope, coll, []string{"county"})
	createDeferIndex("index-7", bucket, scope, coll, []string{"email"})
	createDeferIndex("index-8", bucket, scope, coll, []string{"mobile"})
	createDeferIndex("index-9", bucket, scope, coll, []string{"name"})
}

func BuildIndexes(indexes []string) {
	client, err := secondaryindex.GetOrCreateClient(options.IndexAddr, "test")
	if err != nil {
		panic(err)
	}
	defnIDs := make([]uint64, 0)
	for _, bindex := range indexes {
		bucket, scope, collection, iname, err := processIndexName(bindex)
		if err != nil {
			panic(err)
		}
		index, ok := querycmd.GetIndex(client, bucket, scope, collection, iname)
		if ok {
			defnIDs = append(defnIDs, uint64(index.Definition.DefnId))
		} else {
			err = fmt.Errorf("Index %v/%v/%v/%v unknown", bucket, scope, collection, iname)
			break
		}
	}
	if err == nil {
		err = client.BuildIndexes(defnIDs)
		log.Printf("Index building for: %v, err: %v", defnIDs, err)
	}
}

func processIndexName(indexName string) (string, string, string, string, error) {
	v := strings.Split(indexName, ":")
	if len(v) < 0 {
		return "", "", "", "", fmt.Errorf("invalid index specified : %v", indexName)
	}

	scope := ""
	collection := ""
	bucket := ""
	iname := ""
	if len(v) == 4 {
		bucket, scope, collection, iname = v[0], v[1], v[2], v[3]
	} else if len(v) == 2 {
		bucket, iname = v[0], v[1]
	}
	if scope == "" {
		scope = c.DEFAULT_SCOPE
	}
	if collection == "" {
		collection = c.DEFAULT_COLLECTION
	}
	return bucket, scope, collection, iname, nil
}
