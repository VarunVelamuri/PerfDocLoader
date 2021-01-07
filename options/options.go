package options

import (
	"flag"
	"fmt"
	"os"
)

var Bucket string
var Scope string
var CollPrefix string

var Username string
var Password string
var KVaddress string // Should contain the port number of data service
var IndexAddr string

var NumColl int
var CollGen bool        // Set to true to generage "numColl" number of collections
var InitDocsPerColl int // Initial set of docs in each collection
var IncrOpsPerSec int   // KV ops/sec on the bucket for incremental workload
var LoopIncr bool       // Loop incremental updates
var IndexGen bool       // Generate 10 indexes per collection

func PrintOptions() {
	//log.Printf("Command line options: %v", options)
}

func ArgParse() {

	flag.StringVar(&Bucket, "bucket", "default",
		"buckets to connect")
	flag.StringVar(&Scope, "scope", "_default",
		"Scope in which the collections are to be created")
	flag.StringVar(&CollPrefix, "collPrefix", "collection",
		"Prefix with which the collections are to be created")

	flag.StringVar(&Username, "username", "Administrator",
		"Cluster username")
	flag.StringVar(&Password, "password", "asdasd",
		"Cluster password")
	flag.StringVar(&KVaddress, "kvaddress", "couchbase://127.0.0.1:12000",
		"KV address")
	flag.StringVar(&IndexAddr, "indexAddr", "127.0.0.1:8091",
		"Index address")

	flag.IntVar(&NumColl, "numColl", 10,
		"Number of collections to generate when collGen is set to true")
	flag.BoolVar(&CollGen, "collGen", false,
		"Generate collections with prefix when set to true")
	flag.IntVar(&InitDocsPerColl, "initDocsPerColl", 0,
		"Number of docs to populate for each collection")
	flag.IntVar(&IncrOpsPerSec, "incrOpsPerSec", 1000,
		"Number of ops/sec on the bucket for incremental workload")
	flag.BoolVar(&LoopIncr, "loopIncr", false,
		"loop incremental updates")
	flag.BoolVar(&IndexGen, "indexGen", false,
		"Create and build indexes")
	flag.Parse()
}

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}
