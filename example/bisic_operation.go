package main

import (
	"fmt"
	"time"

	bitcaskkv "github.com/GGjahon/bitcask-kv"
)

func main() {

	db, err := bitcaskkv.Open(bitcaskkv.WithDBDirPath("/tmp/bitcask-kv"))
	if err != nil {
		panic(err)
	}
	key := []byte("name")
	// val := []byte("kkv")
	// err = db.Put(key, val)
	// if err != nil {
	// 	panic(err)
	// }
	value, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))

	// err = db.Delete(key)
	// if err != nil {
	// 	panic(err)
	// }
	time.Sleep(time.Second * 10)
}
