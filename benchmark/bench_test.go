package benchmark

import (
	"math/rand"
	"testing"
	"time"

	bitcaskkv "github.com/GGjahon/bitcask-kv"
	"github.com/GGjahon/bitcask-kv/utils"
	"github.com/stretchr/testify/assert"
)

var db *bitcaskkv.DB

func init() {
	var err error
	db, err = bitcaskkv.Open(bitcaskkv.WithDBDirPath("/tmp/benchmar_testdata"))
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		//Benchmark_Put-4   	   79347	     14683 ns/op	    2358 B/op	       8 allocs/op
		//Benchmark_Put-4          66451         19355 ns/op        2312 B/op          7 allocs/op
		//Benchmark_Put-4          73057         17516 ns/op        2312 B/op          7 allocs/op
		//Benchmark_Put-4          66669         16704 ns/op        2312 B/op          7 allocs/op
		err := db.Put(utils.GetRandomKey(i), utils.GetRandomValue(1024))
		assert.NoError(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		//Benchmark_Get-4          1753261               635.4 ns/op           132 B/op          4 allocs/op
		//Benchmark_Get-4          1866933               646.5 ns/op           132 B/op          4 allocs/op
		//Benchmark_Get-4          1836891               655.1 ns/op           132 B/op          4 allocs/op
		_, err := db.Get(utils.GetRandomKey(rand.Int()))
		if err != nil && err != bitcaskkv.ErrKeyIsNotFound {
			b.Fatal(err)
		}
	}
}
