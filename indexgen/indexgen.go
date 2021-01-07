package indexgen

import (
	"github.com/couchbase/PerfDocLoader/options"
	c "github.com/couchbase/indexing/secondary/common"
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
	createDeferIndex("index-0", bucket, scope, coll, []string{"realm"})
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
